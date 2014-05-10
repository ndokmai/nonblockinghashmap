use std::hash;
use std::hash::Hash;
use std::hash::sip::SipState;
use std::cast::transmute;

// ---Key-or-Value Slot Type--------------------------------------------------------------------------------
#[deriving(Eq)]
pub enum KeyTypes{
	KeyType,
	KeyTombStone,
	KeyEmpty,
}

pub struct Key<T> {
	pub _keytype: KeyTypes,
	pub _key: *mut T,
}

impl<T: Hash> Key<T> {
	pub fn new(k: T) -> Key<T> {
		Key { _keytype: KeyType, _key: unsafe{ transmute(~k) } }
	}

	pub fn new_empty() -> Key<T> {
		Key { _keytype: KeyEmpty, _key: unsafe{ transmute(0) } }
	}

	pub fn new_tombstone() -> Key<T> {
		Key { _keytype: KeyTombStone, _key: unsafe{ transmute(0) } }
	}

	pub fn is_empty(&self) -> bool {
		self._keytype==KeyEmpty
	}
	pub fn is_tombstone(&self) -> bool {
		self._keytype==KeyTombStone
	}

	pub fn keytype(&self) -> KeyTypes{
		self._keytype
	}

	pub fn get_key(&self) -> *mut T {
		assert!(self._key as int != 0);
		self._key
	}

	// ---Hash Function--------------------------------------------------------------------------------------
	pub fn hash(&self) -> u64 {
		let mut h = hash::hash(unsafe {&(*self._key)});	
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

impl<T: Eq + Hash> Eq for Key<T>{
	fn eq(&self, other: &Key<T>) -> bool{
		if self._keytype!=other._keytype { return false; }
		if self._keytype==KeyEmpty && other._keytype==KeyEmpty { return true; } 
		if self._keytype==KeyTombStone&& other._keytype==KeyTombStone { return true; }
		assert!(self._key as uint !=0 && other._key as uint != 0);
		if self._key==other._key || unsafe {(*self._key)==(*other._key)}  { return true; }
		return false;
	}	
}

#[deriving(Eq)]
pub enum ValueTypes{
	ValueType,
	ValueTombStone,
	ValueEmpty,
}

pub struct Value<T> {
	pub _valuetype: ValueTypes,
	pub _value: *mut T,
	pub _is_prime: bool
}

impl<T> Value<T> {
	pub fn new(v: T) -> Value<T> {
		Value { _valuetype: ValueType, _value: unsafe{ transmute(~v) }, _is_prime: false }
	}

	pub fn new_empty() -> Value<T> {
		Value { _valuetype: ValueEmpty, _value: unsafe{ transmute(0) }, _is_prime: false }
	}

	pub fn new_tombstone() -> Value<T> {
		Value { _valuetype: ValueTombStone, _value: unsafe{ transmute(0) }, _is_prime: false }
	}

	pub fn new_tombprime() -> Value<T> {
		Value { _valuetype: ValueTombStone, _value: unsafe{ transmute(0) }, _is_prime: true }
	}

	pub fn new_prime(v: T) -> Value<T> {
		Value { _valuetype: ValueType, _value: unsafe{ transmute(~v) }, _is_prime: true }
	}

	pub fn is_empty(&self) -> bool {
		assert!((self._value as int == 0) == (self._valuetype==ValueEmpty));
		self._valuetype==ValueEmpty
	}

	pub fn is_tombstone(&self) -> bool{
		self._valuetype==ValueTombStone
	}

	pub fn is_prime(&self) -> bool {
		self._is_prime	
	}
	pub fn is_tombprime(&self) -> bool {
		self.is_prime() && self.is_tombstone()
	}

	pub fn get_prime(&self) -> *mut Value<T>{
		assert!(!self.is_prime());
		unsafe {
			transmute(~Value { _valuetype: self._valuetype, _value: self._value, _is_prime: true })
		}
	}

	pub fn get_unprime(&self) -> *mut Value<T>{
		assert!(self.is_prime());
		unsafe {
			transmute(~Value { _valuetype: self._valuetype, _value: self._value, _is_prime: false })
		}
	}

	pub fn valuetype(&self) -> ValueTypes {
		self._valuetype
	}

	pub fn get_value(&self) -> *mut T {
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
		if self._valuetype!=other._valuetype { return false; }
		if self._valuetype==ValueEmpty && other._valuetype==ValueEmpty { return true; } 
		if self._valuetype==ValueTombStone && other._valuetype==ValueTombStone && self._is_prime==other._is_prime { return true; }
		assert!(self._value as uint !=0 && other._value as uint != 0);
		if (self._value==other._value || unsafe {(*self._value)==(*other._value)} ) && self._is_prime==other._is_prime { return true; }
		return false;
	}	
}

