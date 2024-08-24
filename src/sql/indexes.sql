create unique index if not exists single_chain_config on config(chain_id);

create index if not exists logs_address_topic_idx on logs(block_num desc, address, (topics[1]));
create index if not exists logs_topic_idx on logs(block_num desc, (topics[1]));

create extension if not exists btree_gin;
create index if not exists logs_block_num_address_topics_idx on logs using gin (block_num, address, topics);
