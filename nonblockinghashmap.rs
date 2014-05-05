#![crate_id="nonblockinghashmap"]
#![feature(default_type_params)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(globs)]
extern crate time;

use std::hash;
use std::hash::Hash;
use std::hash::sip::SipState;
use std::sync::atomics::{AtomicOption, AtomicPtr, AtomicUint};
use std::sync::atomics::{SeqCst};
use std::cast::transmute;
use std::container::Container;
use time::{ Timespec, get_time };
use std::sync::atomics::fence;
use std::cmp::min;
use std::to_str::ToStr;
use std::fmt::Show;

use keyvalue::{Key, Value, KeyTombStone, ValueTombStone, KeyType, ValueType, KeyEmpty, ValueEmpty};
use kvtable::{KVs, REPROBE_LIMIT};

mod keyvalue;
mod kvtable;

static MIN_SIZE_LOG: uint = 3;
static MIN_SIZE: uint = 1<<MIN_SIZE_LOG;

#[deriving(Eq)]
enum MatchingTypes {
	MatchAll,
	MatchAllNotEmpty,
	MatchValue,
	FromCopySlot
}

// ---Hash Map --------------------------------------------------------------------
pub struct NonBlockingHashMap<K,V> {
	_kvs: AtomicPtr<KVs<K,V>>,
	//_reprobes: AtomicUint,
	_last_resize: Timespec, 
}

impl<K: Eq + Hash,V: Eq> NonBlockingHashMap<K,V> {

	pub fn new() -> NonBlockingHashMap<K,V> {
		NonBlockingHashMap::new_with_size(MIN_SIZE)
	}

	pub fn new_with_size(initial_sz: uint) -> NonBlockingHashMap<K, V> {	
		let mut initial_sz = initial_sz;
		if initial_sz > 1024*1024 {
			initial_sz = 1024*1024;
		}
		let mut i = MIN_SIZE_LOG;
		while 1<<i < initial_sz<<2 { i += 1;
		}

		NonBlockingHashMap {
			_kvs: AtomicPtr::new( unsafe {transmute(~KVs::<K,V>::new(1<<i))}),
			//_reprobes: AtomicUint::new(0),
			_last_resize: get_time()
		}
	}

	pub fn get_table_nonatomic(&self) -> *mut KVs<K,V>{
		self._kvs.load(SeqCst)	
	}

	pub fn resize(&self, kvs: *mut KVs<K,V>) -> *mut KVs<K,V> {
		unsafe {
			fence(SeqCst);
			if (*kvs)._chm.has_newkvs() {
				return (*kvs)._chm._newkvs.load(SeqCst);
			}

			let oldlen: uint = (*kvs).len();
			let sz = (*kvs)._chm._size.load(SeqCst);
			let mut newsz = sz;

			if sz >= oldlen>>2 {
				newsz = oldlen<<1;
				if sz >= oldlen>>1 {
					newsz = oldlen<<2;
				}
			}

			let tm = get_time();
			if newsz <= oldlen && tm.sec <= self._last_resize.sec + 1 && (*kvs)._chm._slots.load(SeqCst) >= sz<<1 {
				newsz = oldlen<<1;			
			}

			if newsz < oldlen {
				newsz = oldlen;
			}

			let mut log2: uint = MIN_SIZE_LOG;
			while 1<<log2 < newsz { log2 += 1 };

			if (*kvs)._chm.has_newkvs() {
				return (*kvs)._chm._newkvs.load(SeqCst);
			}

			let mut newkvs: *mut KVs<K,V> = transmute(~KVs::<K,V>::new(1<<log2));

			if (*kvs)._chm.has_newkvs() {
				return (*kvs)._chm._newkvs.load(SeqCst);
			}

			let oldkvs = (*kvs)._chm._newkvs.load(SeqCst);
			if (*kvs)._chm._newkvs.compare_and_swap(oldkvs, newkvs, SeqCst)==oldkvs{
				(*kvs)._chm._has_newkvs = true;
				self.rehash();
			}
			else {
				newkvs = (*kvs)._chm._newkvs.load(SeqCst);
			}
			return newkvs;
		}

	}
	pub fn put<'a>(&mut self, key: K, newval: V) -> &'a V{
		self.put_if_match(key, newval, MatchAll, None)
	}

	pub fn put_if_match<'a>(&mut self, key: K, newval: V, matchingtype: MatchingTypes, expval: Option<V>) -> &'a V{
		let table = self.get_table_nonatomic();
		self.put_if_match_to_kvs(table, key, newval, matchingtype, expval)
	}

	pub fn put_if_match_to_kvs<'a>(&mut self, kvs: *mut KVs<K,V>, key: K, newval: V, matchingtype: MatchingTypes, expval: Option<V>) -> &'a V{
		unsafe {
			let new_expval: Option<*mut Value<V>> = {
				if expval.is_some(){
					Some(transmute(~Value::<V>::new(expval.unwrap())))
				}
				else { None }
			};
			let returnval = self.put_if_match_impl(kvs, transmute(~Key::<K>::new(key)), transmute(~Value::<V>::new(newval)), matchingtype, new_expval);
			return &'a *(*returnval)._value;
		}
	}

	pub fn put_if_match_impl(&mut self, kvs: *mut KVs<K,V>, key: *mut Key<K>, putval: *mut Value<V>, matchingtype: MatchingTypes, expval: Option<*mut Value<V>>) -> *mut Value<V> {
		unsafe {
			assert!(!(*putval).is_empty()); // Never put a ValueEmpty type
			assert!(!(*putval).is_prime()); // Never put a Prime type
			assert!(matchingtype!=MatchValue || !expval.is_none()); // If matchingtype==MatchValue then expval must contain something 
			if !expval.is_none() { assert!(!(*expval.unwrap()).is_prime()); } // Never expect a Prime type

			let fullhash = (*key).hash(); 
			let len = (*kvs).len();
			let mut idx = (fullhash & (len-1) as u64) as uint;
			let mut reprobe_cnt: uint = 0;
			let mut k = (*kvs).get_key_nonatomic_at(idx);
			let mut v = (*kvs).get_value_nonatomic_at(idx);
			// Determine if expval is empty
			let mut expval_not_empty = false;
			if matchingtype==MatchValue {
				if !(*expval.unwrap()).is_empty() { 
					expval_not_empty = true;
				}
			}	
			else { expval_not_empty = true; }
			// Probing/Re-probing
			loop {
				if (*k).is_empty() { // Found an available key slot
					if (*putval).is_tombstone() { return putval; } // Never change KeyEmpty to KeyTombStone 
					if (*kvs)._ks[idx].compare_and_swap(k, key, SeqCst)==k{ // Add key to the slot
						(*kvs)._chm._slots.fetch_add(1, SeqCst);	// Add 1 to the number of used slots
						(*kvs)._hashes[idx] = fullhash;
						break;
					}
					k = (*kvs).get_key_nonatomic_at(idx);
					v = (*kvs).get_value_nonatomic_at(idx);
					assert!(!(*k).is_empty());
				} 
				fence(SeqCst);
				if k==key || (*k)==(*key)  {
					break;		
				}
				// Start re-probing
				reprobe_cnt += 1;
				if reprobe_cnt >= REPROBE_LIMIT || 
					(*key).is_tombstone() // Enter state {KeyTombStone, Empty}; steal exucution path for optimization; let helper save the day.
					{
						let newkvs = self.resize(kvs); 
						if expval_not_empty { self.help_copy(); }
						return self.put_if_match_impl(newkvs, key, putval, matchingtype,  expval); // Put in the new table instead
					} 
				idx = (idx+1)&(len-1);
				k = (*kvs).get_key_nonatomic_at(idx);
				v = (*kvs).get_value_nonatomic_at(idx);
			}
			// End probe/re-probing

			if (*putval)==(*v) { return v; } // Steal path exucution for optimization; let helper save the day.
			if (*kvs)._chm.has_newkvs() && 
				(( (*v).is_tombstone() && (*kvs).table_full(reprobe_cnt) ) || // Resize if the table is full.
				 (*v).is_prime()) // I don't understand this, but I take it from the original code anyway. It is some sort of invalid state caused by compilier's optimization.
				{
					self.resize(kvs);		
				}
			if (*kvs)._chm.has_newkvs() { // Check for the last time if kvs is the newest table
				let expval_is_empty = {
					match expval {
						Some(val) => {
							if (*val).is_empty() { true }
							else { false }
						} 
						None => true 
					}
				};
				let copied_kvs = self.copy_slot_and_check(kvs, idx, !expval_is_empty); // If expval is empty then don't help (expval is empty only if this function is called from copy_slot)
				return self.put_if_match_impl(copied_kvs, key, putval, matchingtype, expval);
			}

			// This table is the newest, so we can start entering the state machine.
			loop {
				assert!(!(*v).is_prime()); // If there is a Prime than this cannot be the newest table.
				if matchingtype!=MatchAll && // If expval is not a wildcard
					( matchingtype!=MatchAllNotEmpty|| (*v).is_tombstone() || (*v).is_empty() ) // If expval is not a TombStone or Empty
					{
						assert!(!expval.is_none());
						if v!=expval.unwrap() && // if v!= expval (pointer)
							!((*v).is_empty() && (*expval.unwrap()).is_tombstone()) && // If we expect a TombStone and v is empty, it should be a match.
								((*expval.unwrap()).is_empty() || (*expval.unwrap())!=(*v)) // expval==Empty or *expval==*v
								{
									return v; // do nothing, just return the old value.
								}
					}

				// Finally, add some values.
				if (*kvs)._vs[idx].compare_and_swap(v, putval, SeqCst)==v {
					if expval_not_empty {
						if ((*v).is_empty() || (*v).is_tombstone()) && !(*putval).is_tombstone() { (*kvs)._chm._size.fetch_add(1, SeqCst); }
						if !((*v).is_empty() || (*v).is_tombstone()) && (*putval).is_tombstone() { (*kvs)._chm._size.fetch_sub(1, SeqCst); }
					}
					if (*v).is_empty() && expval_not_empty { return transmute(~Value::<V>::new_tombstone()) }
					else { return v; }
				}
				v = (*kvs).get_value_nonatomic_at(idx);
				if (*v).is_prime(){
					let copied_kvs = self.copy_slot_and_check(kvs, idx, expval_not_empty);
					return self.put_if_match_impl(copied_kvs, key, putval, matchingtype, expval);
				}
			}
		}
	}

	pub fn get<'a>(&'a mut self, key: K) -> Option<&'a V>{
		unsafe {
			let table = self.get_table_nonatomic();
			let returnvalue = self.get_impl(table, transmute(~Key::<K>::new(key)));
			if returnvalue.is_some() {
				return Some(&'a *((*returnvalue.unwrap())._value));
			}
			else { return None; }
		}
	}

	// Compute hash only once
	pub fn get_impl(&mut self, kvs: *mut KVs<K,V>, key: *mut Key<K>) -> Option<*mut Value<V>> {
		unsafe {self.get_impl_supply_hash(kvs, key, (*key).hash())}
	}

	pub fn get_impl_supply_hash(&mut self, kvs: *mut KVs<K,V>, key: *mut Key<K>, fullhash: u64) -> Option<*mut Value<V>> {
		unsafe {
			let len = (*kvs).len();
			let mut idx = (fullhash & (len-1) as u64) as uint;
			let mut reprobe_cnt: uint = 0;
			loop {
				let k = (*kvs).get_key_nonatomic_at(idx);
				let v = (*kvs).get_value_nonatomic_at(idx);
				if (*k).is_empty() { return None }
				fence(SeqCst);
				if (*k)==(*key) { 
					if !(*v).is_prime() {
						if (*v).is_tombstone() { return None }	
						else { return Some(v) }
					}
					else {
						let table = self.copy_slot_and_check(kvs, idx, true);
						return self.get_impl_supply_hash(table, key, fullhash);
					}
				}
				reprobe_cnt += 1;
				if reprobe_cnt >= REPROBE_LIMIT || (*k).is_tombstone() {
					if (*kvs)._chm.has_newkvs() {
						self.help_copy();
						return self.get_impl_supply_hash((*kvs)._chm.get_newkvs_nonatomic(), key, fullhash);
					}
					else { return None; }
				}
				idx = (idx+1) & (len-1);
			}
		}

	}

	pub fn copy_slot_and_check(&mut self, oldkvs: *mut KVs<K,V>, idx: uint, should_help: bool) -> *mut KVs<K,V>{
		fence(SeqCst);
		unsafe {
			assert!( (*oldkvs)._chm.get_newkvs_nonatomic() as int != 0 );
			if self.copy_slot(idx, oldkvs) {
				self.copy_check_and_promote(oldkvs, 1);
			}

			if should_help {
				self.help_copy();
				return (*oldkvs)._chm.get_newkvs_nonatomic();
			}
			else {
				return (*oldkvs)._chm.get_newkvs_nonatomic();
			}
		}

	}

	pub fn copy_check_and_promote(&mut self, oldkvs: *mut KVs<K,V>, work_done: uint){
		unsafe{
			let oldlen = (*oldkvs).len();
			let mut copy_done = (*oldkvs)._chm._copy_done.load(SeqCst);
			assert!(copy_done + work_done <= oldlen);
			if work_done > 0 {
				while (*oldkvs)._chm._copy_done.compare_and_swap(copy_done, copy_done + work_done, SeqCst)!=copy_done {
					copy_done = (*oldkvs)._chm._copy_done.load(SeqCst);
				}
				assert!(copy_done + work_done <= oldlen);
			}

			if copy_done + work_done == oldlen &&
				self._kvs.load(SeqCst) == oldkvs &&
					(self._kvs.compare_and_swap(oldkvs, ((*oldkvs)._chm.get_newkvs_nonatomic()), SeqCst)==oldkvs) {
						self._last_resize = get_time();
					}
		}
	}

	pub fn copy_slot(&mut self, idx: uint, oldkvs: *mut KVs<K,V>) -> bool{
		unsafe {

			let mut key = (*oldkvs).get_key_nonatomic_at(idx);

			// State transition: {Empty, Empty} -> {KeyTombStone, Empty}
			// ---------------------------------------------------------
			let tombstone_ptr: *mut Key<K> = transmute(~Key::<K>::new_tombstone());
			while (*key).is_empty() {
				if (*oldkvs)._ks[idx].compare_and_swap(key, tombstone_ptr, SeqCst)==key{ // Attempt {Empty, Empty} -> {KeyTombStone, Empty}
					return true;
				}
				key = (*oldkvs).get_key_nonatomic_at(idx);
			}
			// ---------------------------------------------------------

			// Enter state: {KeyTombStone, Empty}
			// ---------------------------------------------------------
			if (*key).is_tombstone() {
				return false;	
			}
			// ---------------------------------------------------------

			// State transition: {Key, Empty} -> {Key, ValueTombPrime} or {Key, ValueTombStone} -> {Key, ValueTombPrime} or {Key, Value}->{Key, Value.get_prime()}
			// -------------------------------------------------------------------------------------------------------
			let tombstone_ptr = Value::<V>::new_tombstone().get_prime();
			let mut oldvalue = (*oldkvs).get_value_nonatomic_at(idx);
			while !(*oldvalue).is_prime(){
				let primed = {
					if (*oldvalue).is_empty() { tombstone_ptr }
					else { (*oldvalue).get_prime() } 
				};
				if (*oldkvs)._vs[idx].compare_and_swap(oldvalue, primed, SeqCst)==oldvalue {
					if (*primed).valuetype()==ValueTombStone { return true; } // Transition: {Key, Empty} -> {Key, ValueTombPrime} or {Key, ValueTombStone} -> {Key, ValueTombPrime}
					else { // Transition: {Key, Value} -> {Key, Value'}
						oldvalue = primed; 
						break;
					}
				}
				oldvalue = (*oldkvs).get_value_nonatomic_at(idx);
			}
			// -------------------------------------------------------------------------------------------------------

			let tombprime = Value::<V>::new_tombprime();

			// Enter state: {Key, ValueTombPrime}
			// ---------------------------------------------------------
			if (*oldvalue).is_tombprime()  { return false }	
			// ---------------------------------------------------------

			// State transition: {Key, Value.get_prime()} -> {KeyTombStone, ValueTombPrime}
			// ---------------------------------------------------------
			let old_unprimed = (*oldvalue).get_unprime();
			assert!((*old_unprimed)!=tombprime);
			let newkvs = (*oldkvs)._chm.get_newkvs_nonatomic();
			let emptyval: *mut Value<V> = transmute(~Value::<V>::new_empty());

			self.put_if_match_impl(newkvs, key, old_unprimed, MatchValue, Some( emptyval ));

			let tombprime_ptr: *mut Value<V> = transmute(~Value::<V>::new_tombprime());

			// Enter state: {Key, Value.get_prime()} (intermediate)
			oldvalue = (*oldkvs).get_value_nonatomic_at(idx); // Check again, just in case...
			while !(*oldvalue).is_tombprime() {
				if 	(*oldkvs)._vs[idx].compare_and_swap(oldvalue, tombprime_ptr, SeqCst)==oldvalue {
					return true;
				}
				oldvalue = (*oldkvs).get_value_nonatomic_at(idx);	
			}
			// ---------------------------------------------------------

			return false; // State jump to {KeyTombStone, ValueTombPrime} for threads that lost the competition
		}
	}

	pub fn help_copy(&mut self){
		//     unsafe {
		//if (*self.get_table_nonatomic())._chm.has_newkvs(){
		//let kvs: *mut KVs<K,V> = self.get_table_nonatomic();
		//self.help_copy_impl(kvs, false);
		//}
		//}
	}

	pub fn help_copy_impl(&mut self, oldkvs: *mut KVs<K,V>, copy_all: bool){
		fence(SeqCst);
		unsafe {
			assert!((*oldkvs)._chm.has_newkvs());
			let oldlen: uint = (*oldkvs).len();
			let min_copy_work = min(oldlen, 1024);
			let mut panic_start = false;
			let mut copy_idx = -1;

			while (*oldkvs)._chm._copy_done.load(SeqCst) < oldlen {
				if !panic_start{
					copy_idx = (*oldkvs)._chm._copy_idx.load(SeqCst);
					while copy_idx < oldlen<<1 && 
						(*oldkvs)._chm._copy_idx.compare_and_swap(copy_idx, copy_idx + min_copy_work, SeqCst)!=copy_idx{
							copy_idx = (*oldkvs)._chm._copy_idx.load(SeqCst);
						}
					if copy_idx >= oldlen<<1 {
						panic_start = true;
					}
				}
				for i in range (0, min_copy_work){
					self.copy_slot_and_check( oldkvs, (copy_idx+i)&(oldlen-1), false );
				}
				//let mut work_done = 0;
				//for i in range (0, min_copy_work){
				//if self.copy_slot( (copy_idx+i)&(oldlen-1), oldkvs ){
				//work_done += 1;
				//}
				//}
				//if work_done > 0 {
				//self.copy_check_and_promote(oldkvs, work_done);
				//}

				copy_idx += min_copy_work;

				if !copy_all&& !panic_start {
					return;
				}
			}
			//self.copy_check_and_promote(oldkvs, 0);

		}
	}


	pub fn get_kvs_level(&self, level: uint) -> Option<*mut KVs<K,V>>{
		NonBlockingHashMap::get_kvs_level_impl(self.get_table_nonatomic(), level)
	}

	pub fn get_kvs_level_impl(kvs: *mut KVs<K,V>, level: uint) -> Option<*mut KVs<K,V>>{
		unsafe{
			if kvs as int==0 { return None; }
			if level==0 { 
				return Some(kvs); 
			}
			else { return NonBlockingHashMap::get_kvs_level_impl((*kvs)._chm.get_newkvs_nonatomic(), level-1); }
		}
	}



	pub fn fast_keyeq(k: *mut Key<K>, hashk: u64, key: *mut Key<K>, hashkey: u64) -> bool {
		unsafe{
			k==key || 
				((hashk==0 || hashk==hashkey) &&
				 !(*k).is_tombstone() &&
				 (*key)==(*k))
		}

	}



	pub fn rehash(&self){
	}
}

impl<K,V> Container for NonBlockingHashMap<K,V>{
	fn len(&self) -> uint{
		unsafe {(*self._kvs.load(SeqCst)).len()}
	}	
}

// debuging functions
pub fn print_table<K: Eq + Hash + Show,V: Eq + Show>(table: &NonBlockingHashMap<K,V>){
	print_kvs(table.get_table_nonatomic());
}
pub fn print_all<K: Eq + Hash + Show,V: Eq + Show>(table: &NonBlockingHashMap<K,V>){
	unsafe {
		let mut kvs = table.get_table_nonatomic();
		let mut i = 0;
		while kvs as int != 0  {
			println!("---Table {}---", i);
			print_kvs(kvs);
			i+=1;
			kvs = (*kvs)._chm.get_newkvs_nonatomic();
		}

	}

}
pub fn print_kvs<K: Eq + Hash + Show,V: Eq + Show>(kvs: *mut KVs<K,V>){
	unsafe{
		for i in range(0, (*kvs).len()){
			print!("{}: ({}, ", i, key_to_string((*kvs).get_key_nonatomic_at(i)));
			print!("{}, ",value_to_string((*kvs).get_value_nonatomic_at(i)));
			println!("{})",(*kvs)._hashes[i]);
		}
	}

}


pub fn key_to_string<K: Eq + Hash + Show>(key: *mut Key<K>) -> ~str{
	unsafe {
		match (*key).keytype() {
			KeyTombStone => { ~"TOMBSTONE" }
			KeyEmpty => { ~"EMPTY" }
			KeyType => { 
				assert!((*key)._key as int != 0);
				(*(*key)._key).to_str()
			}
		}
	}
}

pub fn value_to_string<V: Eq + Show>(value: *mut Value<V>) -> ~str{
	unsafe {
		match (*value).valuetype() {
			ValueTombStone => {
				if (*value).is_prime() { ~"TOMBPRIME" }
				else { ~"TOMBSTONE" }
			}
			ValueEmpty => { ~"EMPTY" }
			ValueType => { 
				assert!((*value)._value as int != 0);
				if (*value).is_prime() { "Prime("+(*(*value)._value).to_str()+")" }
				else { (*(*value)._value).to_str() }
			}
		}
	}
}


/****************************************************************************
 * Tests
 ****************************************************************************/
#[cfg(test)]
mod test {
	use super::{Key, Value, KVs, CHM, NonBlockingHashMap, KeyEmpty, ValueEmpty};
	use std::sync::atomics::{AtomicPtr, AtomicUint};
	use std::sync::atomics::{SeqCst};
	use std::cast::transmute;
	use std::io::timer::sleep;

	#[test]
	fn test_value_prime_swapping() {
		unsafe {
			let value: *mut Value<int> = transmute(~Value::new(10));
			let atomicvalue = AtomicPtr::new(value);
			let valueprime = (*value).get_prime();
			assert!(!(*atomicvalue.load(SeqCst)).is_prime());
			atomicvalue.swap(valueprime, SeqCst);
			assert!((*atomicvalue.load(SeqCst))._value==(*value)._value);
			assert!((*atomicvalue.load(SeqCst)).is_prime());
		}
	}

	#[test]
	#[allow(dead_assignment)]
	fn test_KV_destroy(){
		unsafe {
			let mut p: *mut int = transmute(~5) ;
			{
				let kv = Key::new(10);
				p = kv.get_key() ;
				assert!((*p)==10);
			}
			assert!((*p)!=10);
			assert!((*p)!=5);

			let mut p: *mut int = transmute(~5) ;
			{
				let kv = Value::new(10);
				p = kv.get_value() ;
				assert!((*p)==10);
			}
			assert!((*p)!=10);
			assert!((*p)!=5);
		}	
	}

	#[test]
	fn test_vey_eq(){
		assert!(Key::<int>::new_empty()==Key::<int>::new_empty());
		assert!(Key::<int>::new_tombstone()==Key::<int>::new_tombstone());
		assert!(Key::<int>::new(10)==Key::<int>::new(10));
		assert!(Key::<int>::new(5)!=Key::<int>::new(10));
	}

	#[test]
	fn test_value_eq(){
		unsafe {
			assert!(Value::<int>::new_empty()==Value::<int>::new_empty());
			assert!(Value::<int>::new_tombstone()==Value::<int>::new_tombstone());
			assert!((*Value::<int>::new_tombstone().get_prime())==(*Value::<int>::new_tombstone().get_prime()));
			assert!(Value::<int>::new_tombprime()==(*Value::<int>::new_tombstone().get_prime()));
			assert!(Value::<int>::new_tombprime()==Value::<int>::new_tombprime());
			assert!(Value::<int>::new(10)==Value::<int>::new(10));
			assert!(Value::<int>::new(5)!=Value::<int>::new(10));
			assert!((*Value::<int>::new(10).get_prime())==(*Value::<int>::new(10).get_prime()));
		}
	}

	#[test]
	fn test_KVs_init(){
		let kvs = KVs::<int,int>::new(10);
		unsafe {
			for i in range(0,kvs._ks.len()) {
				assert!((*kvs._ks[i].load(SeqCst)).keytype()==KeyEmpty);
			}
			for i in range(0,kvs._ks.len()) {
				assert!((*kvs._vs[i].load(SeqCst)).valuetype()==ValueEmpty);
			}
		}
	}

	#[test]
	fn test_hashmap_init(){
		let map = NonBlockingHashMap::<int,int>::new_with_size(10);
		assert!(map.len()==16*4);
		unsafe {
			assert!((*map._kvs.load(SeqCst))._chm._newkvs.load(SeqCst) as int == 0);
		}
	}

	#[test]
	fn test_hashmap_resize(){
		let map1 = NonBlockingHashMap::<int,int>::new_with_size(10);
		let kvs = map1._kvs.load(SeqCst);
		map1.resize(kvs);
		unsafe {
			assert!((*(*kvs)._chm._newkvs.load(SeqCst)).len() == 16*4*2);
		}
		let kvs = unsafe {(*kvs)._chm._newkvs.load(SeqCst)};
		map1.resize(kvs);
		unsafe {
			assert!((*(*kvs)._chm._newkvs.load(SeqCst)).len() == 16*4*4);
		}
		let map2 = NonBlockingHashMap::<int,int>::new_with_size(10);
		sleep(2000);
		map2.resize(map2._kvs.load(SeqCst));
		unsafe {
			assert!((*(*map2._kvs.load(SeqCst))._chm._newkvs.load(SeqCst)).len() == 16*4);
		}
	}
}


