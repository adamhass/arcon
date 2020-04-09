// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{config::NEXMarkConfig, source::NEXMarkSource, Bid, Event, NEXMarkEvent};
use arcon::prelude::*;
use arcon::macros::*;

/*
    Query 2 Selection
    Query 2 selects all bids on a set of five items and
    tests the stream system’s selection operation.

    SELECT itemid, price
    FROM bid
    WHERE itemid = 1007 OR
        itemid = 1020 OR
        itemid = 2001 OR
        itemid = 2019 OR
        itemid = 1087;
*/

#[arcon]
pub struct AuctionPrice {
    #[prost(uint32, tag = "1")]
    pub auction: u32,
    #[prost(uint32, tag = "2")]
    pub price: u32,
}

// Filter out events that are bids using a FilterMap operator
#[inline(always)]
fn bid_filter_map(mut event: NEXMarkEvent) -> Option<AuctionPrice> {
    match event.inner() {
        Event::Bid(bid) => {
            [1020,2001,2019,1087]
                .iter()
                .copied()
                .find(|&a| a == bid.auction)
                .and(Some(AuctionPrice{auction: bid.auction, price: bid.price}))
        },
        _ => None,
    }
}

/// ConcurrencyConversion
/// Stream of Bids: Convert bid price from U.S dollars to Euros
///
/// Source(FilterMap -> Bid) -> MapInPlace -> Sink
pub fn q2(debug_mode: bool, nexmark_config: NEXMarkConfig, pipeline: &mut ArconPipeline) {
    let channel_batch_size = pipeline.arcon_conf().channel_batch_size;
    let watermark_interval = pipeline.arcon_conf().watermark_interval;
    let system = pipeline.system();
    let timeout = std::time::Duration::from_millis(500);

    // If debug mode is enabled, we send the data to a DebugNode.
    // Otherwise, just discard the elements using a Mute strategy...
    let channel_strategy = {
        if debug_mode {
            let sink = system.create(move || DebugNode::<AuctionPrice>::new());

            system
                .start_notify(&sink)
                .wait_timeout(timeout)
                .expect("sink never started!");

            let sink_ref: ActorRefStrong<ArconMessage<AuctionPrice>> = sink.actor_ref().hold().expect("no");
            let sink_channel = Channel::Local(sink_ref);

            let channel_strategy = ChannelStrategy::Forward(Forward::with_batch_size(
                sink_channel,
                NodeID::new(1),
                channel_batch_size,
            ));

            channel_strategy
        } else {
            ChannelStrategy::Mute
        }
    };

    // Define Mapper

    /*
    let in_channels = vec![NodeID::new(1)];

    let node_description = String::from("map_node");
    let node_one = q2_node(
        node_description.clone(),
        NodeID::new(0),
        in_channels.clone(),
        channel_strategy.clone(),
    );
    let node_comps = pipeline.create_node_manager(
        node_description,
        (),
        in_channels,
        channel_strategy,
        vec![node_one],
    );
    */
    {
        let system = pipeline.system();
        // Define source context
        //let mapper_ref = node_comps.get(0).unwrap().actor_ref().hold().expect("fail");
        //let channel = Channel::Local(mapper_ref);
        //let channel_strategy = ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1)));
        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Box::new(FilterMap::<NEXMarkEvent, AuctionPrice>::new(&bid_filter_map)),
            Box::new(InMemory::new("src").unwrap()),
        );

        let nexmark_source_comp = system
            .create_dedicated(move || NEXMarkSource::<AuctionPrice>::new(nexmark_config, source_context));

        system.start(&nexmark_source_comp);
    }
}
/*
pub fn q2_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Bid>,
) -> Node<Bid, Bid> {
    #[inline(always)]
    fn map_fn(bid: &mut Bid) {
        bid.price = (bid.price * 89) / 100;
    }

    Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        Box::new(MapInPlace::new(&map_fn)),
        Box::new(InMemory::new("map").unwrap()),
    )
}
*/