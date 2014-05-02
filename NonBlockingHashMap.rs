#![feature(default_type_params)]
#![allow(dead_code)]
#![feature(globs)]
#![allow(unused_imports)]

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

static REPROBE_LIMIT: uint = 10;  
static MIN_SIZE_LOG: uint = 3;
static MIN_SIZE: uint = 1<<MIN_SIZE_LOG;

// ---Key-or-Value Slot Type--------------------------------------------------------------------------------

#[deriving(Eq)]
enum KeyTypes{
	Key = 1,
	KeyTombStone = 2,
	KeyEmpty = 3,
}

struct Key<T> {
	_keytype: KeyTypes,
	_key: *mut T,
}

impl<T: Hash> Key<T> {
	fn new(k: T) -> Key<T> {
		Key { _keytype: Key, _key: unsafe{ transmute(~k) } }
	}

	fn new_empty() -> Key<T> {
		Key { _keytype: KeyEmpty, _key: unsafe{ transmute(0) } }
	}

	fn new_tombstone() -> Key<T> {
		Key { _keytype: KeyTombStone, _key: unsafe{ transmute(0) } }
	}

	fn is_empty(&self) -> bool {
		self._keytype==KeyEmpty
	}

	fn keytype(&self) -> KeyTypes{
		self._keytype
	}

	fn get_key(&self) -> *mut T {
		self._key
	}

	// ---Hash Function--------------------------------------------------------------------------------------
	fn hash(&self) -> u64 {
		let mut h = hash::hash(&self._key);	
		h += (h << 15) ^ 0xffffcd7d;
		h ^= h >> 10;
		h += h << 3;
		h ^= h >> 6;
		h += h << 2 + h << 14;
		return h ^ (h >> 16);
	}
}

impl<T: Hash> Hash for Key<T>{
	fn hash(&self, state: &mut SipState){
		unsafe {(*self._key).hash(state)};
	}
}

#[unsafe_destructor]
impl<T> Drop for Key<T> {
	fn drop(&mut self){
		unsafe {
			let _: ~T = transmute(self._key);
		}
	}
}

impl<T: Eq> Eq for Key<T>{
	fn eq(&self, other: &Key<T>) -> bool{
		self._keytype==KeyEmpty && other._keytype==KeyEmpty ||
			self._keytype==KeyTombStone && other._keytype==KeyTombStone ||
			self._key==other._key || 
			unsafe {(*self._key)==(*other._key)}  
	}	
}

#[deriving(Eq)]
enum ValueTypes{
	Value = 1,
	ValueTombStone = 2,
	ValueEmpty = 3,
}

struct Value<T> {
	_valuetype: ValueTypes,
	_value: *mut T,
	_is_prime: bool
}

impl<T> Value<T> {
	fn new(v: T) -> Value<T> {
		Value { _valuetype: Value, _value: unsafe{ transmute(~v) }, _is_prime: false }
	}

	fn new_empty() -> Value<T> {
		Value { _valuetype: ValueEmpty, _value: unsafe{ transmute(0) }, _is_prime: false }
	}

	fn new_tombstone() -> Value<T> {
		Value { _valuetype: ValueTombStone, _value: unsafe{ transmute(0) }, _is_prime: false }
	}

	fn new_tombprime() -> Value<T> {
		Value { _valuetype: ValueTombStone, _value: unsafe{ transmute(0) }, _is_prime: true }
	}

	fn new_prime(v: T) -> Value<T> {
		Value { _valuetype: Value, _value: unsafe{ transmute(~v) }, _is_prime: true }
	}

	fn is_empty(&self) -> bool {
		self._valuetype==ValueEmpty
	}

	fn is_prime(&self) -> bool {
		self._is_prime	
	}

	fn prime(&self) -> *mut Value<T>{
		assert!(!self.is_prime());
		unsafe {
			transmute(~Value { _valuetype: self._valuetype, _value: self._value, _is_prime: true })
		}
	}

	fn unprime(&self) -> *mut Value<T>{
		assert!(self.is_prime());
		unsafe {
			transmute(~Value { _valuetype: self._valuetype, _value: self._value, _is_prime: false })
		}
	}

	fn valuetype(&self) -> ValueTypes {
		self._valuetype
	}

	fn get_value(&self) -> *mut T {
		self._value
	}
}

#[unsafe_destructor]
impl<T> Drop for Value<T> {
	fn drop(&mut self){
		unsafe {
			let _: ~T = transmute(self._value);
		}
	}
}

impl<T: Eq> Eq for Value<T>{
	fn eq(&self, other: &Value<T>) -> bool{
		(self._valuetype==ValueEmpty && other._valuetype==ValueEmpty) ||
			(self._valuetype==ValueTombStone && other._valuetype==ValueTombStone && self._is_prime==other._is_prime) ||
			(( self._value==other._value || unsafe {(*self._value)==(*other._value)} ) && self._is_prime==other._is_prime)
	}	
}


// ---Hash Table Layer Node -------------------------------------------------------------------------------

struct KVs<K,V> {
	_ks: ~[AtomicPtr<Key<K>>],
	_vs: ~[AtomicPtr<Value<V>>],
	_chm: CHM<K,V>,
	_hashes: ~[u64]
}

impl<K: Hash,V: Hash> KVs<K,V>{
	fn new(table_size: uint) -> KVs<K,V>{
		KVs {
			_ks: {
					 let mut temp:  ~[AtomicPtr<Key<K>>] = ~[];
					 for _ in range(0, table_size) {
						temp.push(AtomicPtr::new( unsafe {transmute(~Key::<K>::new_empty())} ));
					 }
					 temp
				 },
			_vs: {
					 let mut temp:  ~[AtomicPtr<Value<V>>] = ~[];
					 for _ in range(0, table_size) {
						temp.push(AtomicPtr::new( unsafe {transmute(~Value::<V>::new_tombstone())} ));
					 }
					 temp
				 },
			_chm: CHM::<K, V>::new(),
			_hashes: {
					 let mut temp:  ~[u64] = ~[];
					 for _ in range(0, table_size) {
						temp.push(0);
					 }
					 temp
				 },
		}	
	}	

	fn key_nonatomic(&self, idx: uint) -> *mut Key<K> {
		self._ks[idx].load(SeqCst)	
	}

	fn value_nonatomic(&self, idx: uint) -> *mut Value<V> {
		self._vs[idx].load(SeqCst)	
	}
}

impl<K,V> Container for KVs<K,V> {
	fn len(&self) -> uint {
		self._ks.len()
	}
}

#[unsafe_destructor]
impl<K,V> Drop for KVs<K,V> {
	fn drop(&mut self) {
		for i in range(0, self._ks.len()){
			unsafe{
				let _: ~Key<K> = transmute(self._ks[i].load(SeqCst));
				let _: ~Value<V> = transmute(self._vs[i].load(SeqCst));

			}
		}
	}

}

// ---Structure for resizing -------------------------------------------------------

struct CHM<K,V> {
	_newkvs: AtomicPtr<KVs<K,V>>,
	_size: AtomicUint,
	_slots: AtomicUint,
	_copy_done: AtomicUint,
	_copy_idx: AtomicUint,
	//_resizer: AtomicUint,
}

impl<K,V> CHM<K,V> {
	fn new() -> CHM<K,V>{
		CHM {
			_newkvs: AtomicPtr::new( unsafe {transmute(0)}),
			_size: AtomicUint::new(0), 
			_slots: AtomicUint::new(0), 
			_copy_done: AtomicUint::new(0),
			_copy_idx: AtomicUint::new(0)
		}
	}

	fn newkvs_nonatomic(&self) -> *mut KVs<K,V> {
		self._newkvs.load(SeqCst)
	}
}

#[unsafe_destructor]
impl<K,V> Drop for CHM<K,V> {
	fn drop(&mut self) {
		if self._newkvs.load(SeqCst) as int !=0{
			let _: ~KVs<K,V> = unsafe {transmute(self._newkvs.load(SeqCst))};
		}
	}
}

// ---Hash Map --------------------------------------------------------------------
pub struct NonBlockingHashMap<K,V> {
	_kvs: AtomicPtr<KVs<K,V>>,
	_reprobes: AtomicUint,
	_last_resize: Timespec, 
}

impl<K: Eq + Hash,V: Eq + Hash> NonBlockingHashMap<K,V> {

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
			_reprobes: AtomicUint::new(0),
			_last_resize: get_time()
		}
	}

	fn resize(&self, kvs: *mut KVs<K,V>) -> *mut KVs<K,V> {
		unsafe {
			//	volatile read here	
			if (*kvs)._chm._newkvs.load(SeqCst) as int != 0 {
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
			
			if (*kvs)._chm._newkvs.load(SeqCst) as int != 0 {
				return (*kvs)._chm._newkvs.load(SeqCst);
			}

			let mut newkvs: *mut KVs<K,V> = transmute(~KVs::<K,V>::new(1<<log2) );

			if (*kvs)._chm._newkvs.load(SeqCst) as int != 0 {
				return (*kvs)._chm._newkvs.load(SeqCst);
			}

			let oldkvs = (*kvs)._chm._newkvs.load(SeqCst);
			if (*kvs)._chm._newkvs.compare_and_swap(oldkvs, newkvs, SeqCst)==oldkvs{
				self.rehash();	
			}
			else {
				newkvs = (*kvs)._chm._newkvs.load(SeqCst);
			}
			return newkvs;
		}
	}

	#[allow(unused_variable)]
	fn put_if_match(&self, kvs: *mut KVs<K,V>, key: *mut Key<K>, putval: *mut Value<V>, expval: Option<Value<V>>) -> Value<V> {
		unsafe {
			assert!(!(*putval).is_empty());
			assert!(!(*putval).is_prime());
			match expval {
				Some(val) => assert!(!val.is_empty()),
				None => {}
			}
			
			let fullhash = (*key).hash(); 
			let len = (*kvs).len();
			let idx = (fullhash & (len-1) as u64) as uint;
			let reprobe_cnt: uint = 0;
			let k = (*kvs).key_nonatomic(idx);
			let v = (*kvs).value_nonatomic(idx);

			let k: V = transmute(0);
			return Value::<V>::new(k);

		}
	}

	fn help_copy(&mut self, newkvs: *mut KVs<K,V>) -> *mut KVs<K,V>{
		unsafe {
			if (*self._kvs.load(SeqCst))._chm._newkvs.load(SeqCst) as int == 0 {
				return newkvs;
			}
			let thiskvs: *mut KVs<K,V> = self._kvs.load(SeqCst);
			self.help_copy_impl(thiskvs, false);
			return newkvs;
		}

	}

	fn help_copy_impl(&mut self, oldkvs: *mut KVs<K,V>, copy_all: bool){
		//volatile read here!!
		unsafe {
			assert!((*oldkvs)._chm.newkvs_nonatomic() as int != 0);
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
				let mut work_done = 0;
				for i in range (0, min_copy_work){
					if self.copy_slot( (copy_idx+i)&(oldlen-1), oldkvs ){
						work_done += 1;
					}
				}
				if work_done > 0 {
					self.copy_check_and_promote(oldkvs, work_done);
				}

				copy_idx += min_copy_work;

				if !copy_all&& !panic_start {
					return;
				}
			}
			self.copy_check_and_promote(oldkvs, 0);

		}
	}

	fn copy_slot_and_check(&mut self, oldkvs: *mut KVs<K,V>, idx: uint, should_help: bool) -> *mut KVs<K,V>{
		//volatile read here!!
		unsafe {
			assert!( (*oldkvs)._chm.newkvs_nonatomic() as int != 0 );
			if self.copy_slot(idx, oldkvs) {
				self.copy_check_and_promote(oldkvs, 1);
			}

			if should_help {
				return self.help_copy((*oldkvs)._chm.newkvs_nonatomic());
			}
			else {
				return (*oldkvs)._chm.newkvs_nonatomic();
			}
		}

	}

	fn copy_check_and_promote(&mut self, oldkvs: *mut KVs<K,V>, work_done: uint){
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
				(self._kvs.compare_and_swap(oldkvs, ((*oldkvs)._chm.newkvs_nonatomic()), SeqCst)==oldkvs) {
				self._last_resize = get_time();
			}

		}

	}

	fn copy_slot(&self, idx: uint, oldkvs: *mut KVs<K,V>) -> bool{
		unsafe {
			let mut key = (*oldkvs).key_nonatomic(idx);
			let empty = Key::<K>::new_empty();
			let tombstone_ptr: *mut Key<K> = transmute(~Key::<K>::new_tombstone());
			while *key == empty{
				(*oldkvs)._ks[idx].compare_and_swap(key, tombstone_ptr, SeqCst);
				key = (*oldkvs).key_nonatomic(idx);
			}

			let mut oldvalue = (*oldkvs).value_nonatomic(idx);
			while !(*oldvalue).is_prime(){
				let primed = (*oldvalue).prime();	
				if (*oldkvs)._vs[idx].compare_and_swap(oldvalue, primed, SeqCst)==oldvalue {
					if (*oldvalue).valuetype()==ValueTombStone { return true } 
					oldvalue = primed;
					break;
				}
				oldvalue = (*oldkvs).value_nonatomic(idx);
			}
			let tombprime = Value::<V>::new_tombprime();
			if (*oldvalue) == tombprime { return false }	
			
			let old_unprimed = (*oldvalue).unprime();
			assert!((*old_unprimed)!=tombprime);

			let tombstone = Value::<V>::new_tombstone();
			let newkvs = (*oldkvs)._chm.newkvs_nonatomic();
			let copied_into_new: bool = self.put_if_match(newkvs, key, old_unprimed, Some(Value::<V>::new_tombstone()))==tombstone;
			let tombprime_ptr: *mut Value<V> = transmute(~Value::<V>::new_tombprime());
			while (*oldkvs)._vs[idx].compare_and_swap(oldvalue, tombprime_ptr, SeqCst)!=oldvalue{
				oldvalue = (*oldkvs).value_nonatomic(idx);	
			}
			return copied_into_new;
		}
	}



	fn rehash(&self){
	}
}

impl<K,V> Container for NonBlockingHashMap<K,V>{
	fn len(&self) -> uint{
		unsafe {(*self._kvs.load(SeqCst)).len()}
	}	
}



/****************************************************************************
 * Tests
 ****************************************************************************/
#[cfg(test)]
mod test {
	use super::{Key, Value, KVs, CHM, NonBlockingHashMap, KeyEmpty, ValueTombStone};
	use std::sync::atomics::{AtomicPtr, AtomicUint};
	use std::sync::atomics::{SeqCst};
	use std::cast::transmute;
	use std::io::timer::sleep;

	#[test]
	fn test_value_prime_swapping() {
		unsafe {
			let value: *mut Value<int> = transmute(~Value::new(10));
			let atomicvalue = AtomicPtr::new(value);
			let valueprime = (*value).prime();
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
			assert!((*Value::<int>::new_tombstone().prime())==(*Value::<int>::new_tombstone().prime()));
			assert!(Value::<int>::new_tombprime()==(*Value::<int>::new_tombstone().prime()));
			assert!(Value::<int>::new_tombprime()==Value::<int>::new_tombprime());
			assert!(Value::<int>::new(10)==Value::<int>::new(10));
			assert!(Value::<int>::new(5)!=Value::<int>::new(10));
			assert!((*Value::<int>::new(10).prime())==(*Value::<int>::new(10).prime()));
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
				assert!((*kvs._vs[i].load(SeqCst)).valuetype()==ValueTombStone);
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

pub fn main(){
}
