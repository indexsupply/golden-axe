create index if not exists logs_address_topic_idx on logs((topics[1]), address, block_num desc);
create index if not exists logs_topics_2 on logs((topics[2]));
create index if not exists logs_topics_3 on logs((topics[3]));
create index if not exists logs_topics_4 on logs((topics[4]));
create index if not exists logs_block_num on logs(block_num);
