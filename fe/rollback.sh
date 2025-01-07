ssh ubuntu@www 'cp /home/ubuntu/fe.bak /home/ubuntu/golden-axe/target/release/fe'
ssh ubuntu@www 'sudo systemctl restart fe'
