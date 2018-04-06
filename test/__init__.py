import os, pkgutil

if __name__ == "__main__":
	for loader, mod_name, is_pkg in pkgutil.iter_modules([os.path.dirname(__file__)]):
		try:
			mod = loader.find_module(mod_name).load_module(mod_name)
			for item in dir(mod):
                func = getattr(mod, item)
                if callable(func):
                    func()
		except AttributeError:
			pass