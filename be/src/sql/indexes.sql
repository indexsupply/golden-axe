create index if not exists logs_address_topic_idx on logs((topics[1]), address, block_num desc);
create index if not exists logs_topics_2 on logs((topics[2]));
create index if not exists logs_topics_3 on logs((topics[3]));
create index if not exists logs_topics_4 on logs((topics[4]));
create index if not exists logs_block_num on logs(block_num);


create index if not exists txs_block on txs(block_num);
create index if not exists txs_hash on txs(hash);
create index if not exists txs_from on txs("from");
create index if not exists txs_to on txs("to");
create index if not exists txs_calls on txs using gin (calls);

create index if not exists txs_selector
on txs (substring(input, 1, 4))
where input is not null and octet_length(input) >= 4;


create unique index if not exists blocks_chain_num on blocks(chain, num);
create index if not exists blocks_timestamp on blocks(timestamp);
