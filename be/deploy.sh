ssh ubuntu@ga2 'cp /home/ubuntu/golden-axe/target/release/be /home/ubuntu/be.bak'
ssh ubuntu@ga2 'bash -l -c "cd golden-axe && git pull && cargo build -p be --release"'
ssh ubuntu@ga2 'sudo systemctl restart be'
