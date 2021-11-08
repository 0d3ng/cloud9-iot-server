#PENTING
#my_objects.append(MyClass(i))

import os
import importlib 

list_url = []
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module == 'init.py' or module[-3:] != '.py':
        continue
    module_name = module.replace(".py","")    
    mymodule = importlib.import_module("routes."+module_name)
    define_url = mymodule.define_url
    for li in define_url:
    	list_url.append(("/"+module_name+'/'+li[0],mymodule.__dict__[li[1]]))
 