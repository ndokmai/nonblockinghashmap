extern crate time;
extern crate nonblockinghashmap;
extern crate rand;
use	nonblockinghashmap::{NonBlockingHashMap, print_all};

fn main(){
	let newmap = NonBlockingHashMap::<~str,~str>::new_with_size(1000);
	let shared_map = std::sync::arc::UnsafeArc::new(newmap);
	let nthreads = 30;
	let put = 1000;
	let get = 100000;

	unsafe {
		let (noti_chan, noti_recv) = std::comm::channel();
		for n in range(0, nthreads){
			let child_map_put = shared_map.clone();
			let child_map_get = shared_map.clone();
			let noti_chan_clone_put = noti_chan.clone();
			let noti_chan_clone_get = noti_chan.clone();
			spawn( proc() {
				for i in range(0, put){
					(*child_map_put.get()).put("key"+i.to_str(),"value"+i.to_str()+"_t"+n.to_str());
				}
				noti_chan_clone_put.send(());
			} );

			spawn( proc() {
				for i in range(0, get){
					let key ="key"+(i%put).to_str();
					(*child_map_get.get()).get(key);
					//println!("(key, value) = ({}, {})", key.clone(), (*child_map_get.get()).get(key));
				}
				noti_chan_clone_get.send(());
			} );
		}
		for _ in range(0, nthreads*2){
			noti_recv.recv();	
		}
		print_all(&*shared_map.get());
	}
}

