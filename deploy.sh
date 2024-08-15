ssh ubuntu@ga-base-sep-1 'bash -l -c "cd golden-axe && git pull && cargo build --release"'
ssh ubuntu@ga-base-sep-1 'sudo systemctl restart ga'

read -p "continue?: " input
if [ "$input" != "y" ]; then
	exit 1
fi

ssh ubuntu@ga-base-1 'bash -l -c "cd golden-axe && git pull && cargo build --release"'
ssh ubuntu@ga-base-1 'sudo systemctl restart ga'
