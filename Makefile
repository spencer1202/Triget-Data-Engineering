FOLDER=part1
FILENAME=publisher.py

fetch:
	git fetch origin main
	git checkout origin/main -- $(FOLDER)/$(FILENAME)
	# mv $(FOLDER)/$(FILENAME) .	# <-- uncomment this line
	# rm -r $(FOLDER)				# <-- uncomment this line

reset_all: reset_analysis reset_backup

reset_analysis:
	gcloud pubsub subscriptions seek analysis_sub --time=$(date -u +%Y-%m-%dT%H:%M:%SZ)

reset_backup:
	gcloud pubsub subscriptions seek backup_sub --time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
