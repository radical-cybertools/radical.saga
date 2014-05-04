
.PHONY: clean

clean:
	-rm -rf build/ *.egg-info/ temp/ MANIFEST dist/ saga/VERSION pylint.out *.egg VERSION
	make -C docs clean
	find . -name \*.pyc -exec rm -f {} \;

andre:
	source     ~/.virtualenv/saga-python/bin/activate ; \
	    rm -rf ~.virtualenv/saga-python/lib/python*/site-packages/saga-1.0-py2.6.egg/  ; \
	    easy_install . ; \
	    python test/test_engine.py  ; \
	    python examples/jobs/localjobcontainer.py

mark:
	source     ~/.virtualenv/saga/bin/activate ; \
	    rm -rf ~/.virtualenv/saga/lib/python*/site-packages/saga-1.0-py2.6.egg/  ; \
	    easy_install . ; \

# pages: gh-pages
# 
# gh-pages:
# 	make clean
# 	make docs
# 	git add -f docs/build/html/*
# 	git add -f docs/build/html/*/*
# 	git add -f docs/build/doctrees/*
# 	git add -f docs/build/doctrees/*/*
# 	git add -f docs/source/*
# 	git add -f docs/source/*/*
# 	git  ci -m 'regenerate documentation'
# 	git co gh-pages
# 	git rebase devel
# 	git co devel
# 	git push --all

