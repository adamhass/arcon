use arcon::prelude::*;
use std::sync::Arc;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tempfile::NamedTempFile;


#[test]
fn pipeline_collectionRR_mapBC_filterFW_filesink() {
	/*
		Pipeline 1: CollectionSource to FileSink
			CollectionSource (output = i32 ) -> RoundRobin
				Map x^2 -> Broadcast
				Map x^2 -> Broadcast
					Filter <100 -> Forward
					Filter >=100 -> Forward
						FileSink
	*/
	// Create the input and the verification set
	let mut input = Vec::new();
	let mut expected = Vec::new();
	for i in 1..20 {
		input.push(i as i32);
		expected.push(i*i as i32);
	}

    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path().to_string_lossy().into_owned();

	let cfg = KompactConfig::new();
    let system = KompactSystem::new(cfg).expect("KompactSystem");

    let sink = system.create_and_start(move || 
    	LocalFileSink::<i32>::new(&sink_path));

    let sink_channel = Channel::Local(sink.actor_ref());
    let filter_channels = Forward::<i32>::new(sink_channel);
    let filter_channels2 = filter_channels.clone();

    let filter1_weld_code = String::from("|x: i32| x >= 100");
    let filter1_module = Arc::new(Module::new(filter1_weld_code).unwrap());
    let filter2_weld_code = String::from("|x: i32| x < 100");
    let filter2_module = Arc::new(Module::new(filter2_weld_code).unwrap());
            
    let filter1 = system.create_and_start(move || 
    	Filter::<i32>::new(filter1_module, Vec::new(), Box::new(filter_channels.clone())));
    let filter2 = system.create_and_start(move || 
    	Filter::<i32>::new(filter2_module, Vec::new(), Box::new(filter_channels2)));
    
    let filter1_channel = Channel::Local(filter1.actor_ref());
    let filter2_channel = Channel::Local(filter2.actor_ref());
    let map_channels1 = Broadcast::<i32>::new(vec!(filter1_channel, filter2_channel));
    let map_channels2 = map_channels1.clone();

    let map_weld_code = String::from("|x: i32| x * x");
    let map_module1 = Arc::new(Module::new(map_weld_code.clone()).unwrap());
    let map_module2 = Arc::new(Module::new(map_weld_code).unwrap());
    let map1 = system.create_and_start(move || 
    	Map::<i32, i32>::new(map_module1, Vec::new(), Box::new(map_channels1)));
	let map2 = system.create_and_start(move || 
    	Map::<i32, i32>::new(map_module2, Vec::new(), Box::new(map_channels2)));

	let map1_channel = Channel::Local(map1.actor_ref());
	let map2_channel = Channel::Local(map2.actor_ref());
	let source_channels = RoundRobin::<i32>::new(vec!(map1_channel, map2_channel));

	let _ = system.create_and_start(move || 
    	CollectionSource::<i32>::new(input, Box::new(source_channels)));

    std::thread::sleep(std::time::Duration::from_secs(2));

	// Read the file
    let file = File::open(sink_file.path()).expect("no such file");
    let buf = BufReader::new(file);
    let mut result: Vec<i32> = buf
        .lines()
        .map(|l| l.unwrap().parse::<i32>().expect("could not parse line"))
        .collect();

    result.sort(); // The pipeline split means the elements may become unsorted
    assert_eq!(result, expected);
    let _ = system.shutdown();
}