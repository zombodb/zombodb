DIST_DIR := dist

.PHONY: build
build:
	@echo "====> build the book"
	mdbook build

deploy:
	@echo "====> Setup GitHub Pages Branch"
	git fetch --all
	git worktree add $(DIST_DIR)/ gh-pages
	rm -rf $(DIST_DIR)/*

	cp -rp build/* $(DIST_DIR)/

	@echo "====> Deploy to Github"
	cd $(DIST_DIR) && \
		git update-ref -d refs/heads/gh-pages && \
		git add -A && \
		git commit -m "deployed on $(shell date) by ${USER}" && \
		git push --force origin gh-pages

	rm -rf $(DIST_DIR)
	git worktree prune

clean:
	@echo "=====> clean the build directory"
	rm -Rf build $(DIST_DIR)
	git worktree prune
