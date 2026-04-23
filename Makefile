FOLDER=part1
FILENAME=publisher.py

fetch:
	git fetch origin main
	git checkout origin/main -- $(FOLDER)/$(FILENAME)
	# mv $(FOLDER)/$(FILENAME) .	# <-- uncomment this line
	# rm -r $(FOLDER)				# <-- uncomment this line
