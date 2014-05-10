use std::sync::atomics::{AtomicPtr, AtomicUint};
use std::cast::transmute;
use std::hash::Hash;
use keyvalue::{Key, Value};
use std::sync::atomics::{SeqCst};
mod keyvalue;

pub static REPROBE_LIMIT: uint = 10;  
//fn main(){

//}

// ---Hash Table Layer Node -------------------------------------------------------------------------------
pub struct KVs<K,V> {
	pub _ks: ~[AtomicPtr<Key<K>>],
	pub _vs: ~[AtomicPtr<Value<V>>],
	pub _chm: CHM<K,V>,
	pub _hashes: ~[u64]
}

impl<K: Hash,V> KVs<K,V>{
	pub fn new(table_size: uint) -> KVs<K,V>{
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
							  temp.push(AtomicPtr::new( unsafe {transmute(~Value::<V>::new_empty())} ));
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

	pub fn get_key_nonatomic_at(&self, idx: uint) -> *mut Key<K> {
		self._ks[idx].load(SeqCst)	
	}

	pub fn get_value_nonatomic_at(&self, idx: uint) -> *mut Value<V> {
		self._vs[idx].load(SeqCst)	
	}

	pub fn table_full(&self, reprobe_cnt: uint) -> bool{
		reprobe_cnt >= REPROBE_LIMIT &&
			self._chm._slots.load(SeqCst) >= self.len()
	}

	pub fn reprobe_limit(&self) -> uint{
		REPROBE_LIMIT + self.len()<<2	
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

pub struct CHM<K,V> {
	pub _newkvs: AtomicPtr<KVs<K,V>>,
	pub _size: AtomicUint,
	pub _slots: AtomicUint,
	pub _copy_done: AtomicUint,
	pub _copy_idx: AtomicUint,
	pub _has_newkvs: bool,
	//_resizer: AtomicUint,
}

impl<K,V> CHM<K,V> {
	pub fn new() -> CHM<K,V>{
		CHM {
			_newkvs: AtomicPtr::new( unsafe {transmute(0)}),
			_size: AtomicUint::new(0), 
			_slots: AtomicUint::new(0), 
			_copy_done: AtomicUint::new(0),
			_copy_idx: AtomicUint::new(0),
			_has_newkvs: false,
		}
	}

	pub fn get_newkvs_nonatomic(&self) -> *mut KVs<K,V> {
		self._newkvs.load(SeqCst)
	}

	pub fn has_newkvs(&self) -> bool {
		assert!((self._newkvs.load(SeqCst) as int != 0) == self._has_newkvs);
		self._has_newkvs
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
