ssh ubuntu@ga1 'bash -l -c "cd golden-axe-test && git fetch && git reset --hard origin/test && cargo build -p be --release"'
ssh ubuntu@ga1 'sudo systemctl restart be-test'
