import sys
import random
data = {
	"lq":random.randint(0,100),
	"x":random.randint(4000,10000) / 100,
	"y":random.randint(4000,10000) / 100,
	"z":random.randint(4000,10000) / 100
}
exec("import math\r\nimport random")
c = eval("math.sqrt(pow(abs(int(data['x'])), 2)+pow(abs(int(data['y'])), 2)+pow(abs(int(data['z'])), 2))")
print(c)
sys.stdout.flush()

f = 'accel'
d = {
    'pre':"import math\r\nimport random",
    'process':"math.sqrt(pow(abs(int(var[0])), 2)+pow(abs(int(var[1])), 2)+pow(abs(int(data[var[2]])), 2))",
    'var':['x','y','z']
}

def preproces(insert,data):
    # try: 
        exec(data['pre'])
        var = []
        for x in range(len(data['var'])):
            if str(data['var'][x]) in insert:
                var.append(insert['x'])
            else:
                var.append(0)
        print(var)
        return eval(data['process'])
    # except:
    #     return 0 

res = preproces(data,d)
print(res)
sys.stdout.flush()