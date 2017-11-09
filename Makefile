
VPATH = cubedash/static


base.sass: base.css
	sass -t compact --no-cache cubedash/static/$@ $< 

