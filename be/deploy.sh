ssh ubuntu@ga1 'cp /home/ubuntu/golden-axe/target/release/be /home/ubuntu/be.bak'
ssh ubuntu@ga1 'bash -l -c "cd golden-axe && git pull && cargo build -p be --release"'
ssh ubuntu@ga1 'sudo systemctl restart be'
