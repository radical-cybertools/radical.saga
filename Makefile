
.PHONY: all docs clean

all: docs

docs:
	make -C docs html

pylint:
	@for f in `find saga/ -name \*.py`; do \
	  res=`pylint -r n -f text $$f 2>&1 | grep -e '^[FE]'` ;\
		test -z "$$res" || ( \
		     echo '----------------------------------------------------------------------' ;\
		     echo $$f ;\
		     echo '-----------------------------------'   ;\
				 echo $$res | sed -e 's/ \([FEWRC]:\)/\n\1/g' ;\
				 echo \
		) \
	done

clean:
	-rm -rf build/ saga.egg-info/ temp/
	make -C docs clean
	find . -name \*.pyc -exec rm -f {} \;

andre:
	source     ~/.virtualenv/saga-python/bin/activate ; \
	    rm -rf ~.virtualenv/saga-python/lib/python*/site-packages/saga-1.0-py2.6.egg/  ; \
	    easy_install . ; \
	    python test/test_engine.py  ; \
	    python examples/jobs/localjobcontainer.py

mark:
	# rm -rf ~/.virtualenv/saga-python ;\
  #   virtualenv-2.6 --no-site-packages ~/.virtualenv/saga-python ; \
	# source     ~/.virtualenv/saga-python/bin/activate ; \
	# easy_install .

	source     ~/.virtualenv/saga-python/bin/activate ; \
	    rm -rf ~.virtualenv/saga-python/lib/python*/site-packages/saga-1.0-py2.6.egg/  ; \
	    easy_install .

pages: gh-pages

gh-pages:
	make clean
	make docs
	git add -f docs/build/html/*
	git add -f docs/build/html/*/*
	git add -f docs/build/doctrees/*
	git add -f docs/build/doctrees/*/*
	git add -f docs/source/*
	git add -f docs/source/*/*
	git  ci -m 'regenerate documentation'
	git co gh-pages
	git rebase devel
	git co devel
	git push --all

