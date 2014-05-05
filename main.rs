extern crate time;
extern crate rand;
extern crate nonblockinghashmap;
use	nonblockinghashmap::{NonBlockingHashMap, print_all};

#[allow(unused_unsafe)]
fn main(){
	unsafe {
		let newmap = NonBlockingHashMap::<~str,~str>::new();
		let shared_map = std::sync::arc::UnsafeArc::new(newmap);
		let (noti_chan, noti_recv) = std::comm::channel();
		let mut r = rand::task_rng();
		let nthreads = 1;
		let rwnum = 10000;
		//let writeinitial = rwum>>2;

		let readpercent = 99;
		//let readcnt =

		for n in range(0, nthreads){
			let child_map = shared_map.clone();
			let noti_chan_clone = noti_chan.clone();
			spawn( proc() {
				for i in range(0, 100){
					(*child_map.get()).put("key"+i.to_str(), n.to_str()+"_value"+i.to_str());
				}
				noti_chan_clone.send(());
			} );

		}
		for _ in range(0, nthreads){
			noti_recv.recv();	
		}
		print_all(&(*shared_map.clone().get()));
		let mut reader = std::io::stdio::stdin();
		loop {
			print!("Key: ");
			let input: ~str = reader.read_line().unwrap().trim().to_owned();
			println!("Value: {}", (*shared_map.get()).get(input));
			
		}
		//newmap.help_copy();
		//print_all(&newmap);

		//newmap.help_copy();
		//print_all(&newmap);
	}
}

