ssh ubuntu@www 'bash -l -c "cd golden-axe && git pull && cargo build -p fe --release"'
ssh ubuntu@www 'sudo systemctl restart fe'
