import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import userController
from datetime import datetime  
from datetime import timedelta

users = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count','count'],
    ['detail/','detail'],
    ['login/','login'],
    ['pass/','changepass'],
    ['activation/','activation'],
    ['forgetpassword/','forgetpassword'],
    ['edit/','update'],
    ['delete/','delete']
]
####['delete/([^/]+)','delete']
class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)        
    #check email
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if cloud9Lib.validEmail(data['email']) == False:
        response = {"status":False, "message":"Email Not Valid",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    # if 'username' not in data:
    #     response = {"status":False, "message":"Username Not Found",'data':json.loads(self.request.body)}               
    #     self.write(response)
    #     return

    result = userController.findOne({"email":data['email']})
    if result['status']:        
        response = {"status":False, "message":"Dupplicates email",'data':json.loads(self.request.body)} 
        self.write(response)
        return

    # result = userController.findOne({"username":data['username']})
    # if result['status']:
    #     response = {"status":False, "message":"Dupplicates username",'data':json.loads(self.request.body)} 
    #     self.write(response)
    #     return   

    if 'password' not in data:
        data['password'] = cloud9Lib.randomString()
    
    passwordNoEnct = data['password']
    data['password'] = cloud9Lib.encrypt(data['password'])

    if 'sendotp' in data:
        data['otp'] = cloud9Lib.randomString(6)
        data['expired_otp'] = datetime.utcnow() + timedelta(minutes=5) 

    if 'sendpassword' in data:
        data['active'] = True

    if 'sendlink' in data:
        data['otp'] = cloud9Lib.randomString(6)

    insert = userController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        if 'sendotp' in data:
            html = """\
            <html>
              <body>
                <p>Thank for register in our service,<br>
                   This is your otp to validation your account?<br>
                </p> 
                <p>
                    OTP : <b>"""+data['otp']+ """</b>
                </p>
                <br/>
                <br/>
                <br/>   
                <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                <div><span style='margin: 0;'>Administrator</span></div>
              </body>
            </html>
            """
            mail.send(data['email'],'Registration Account',html)

        if 'sendpassword' in data:
            html = """\
            <html>
              <body>
                <p>Thank for register in our service,<br>
                   This is your password account?<br>
                </p> 
                <p>
                    Password : <b>"""+passwordNoEnct+ """</b>
                </p>
                <br/>
                <br/>
                <br/>   
                <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                <div><span style='margin: 0;'>Administrator</span></div>
              </body>
            </html>
            """
            mail.send(data['email'],'Registration Account',html)

        if 'sendlink' in data:
            email = data['email']
            email = email.replace("@","518ed29525738cebdac49c49e60ea9d3",1)
            link = data['link']+email+"/"+data['otp']
            html = """\
            <html>
              <body>
                <p>Thank for register in our service,<br>
                   This is your activation link?<br>
                </p> 
                <p>
                    Activation Link : <b>"""+link+ """</b>
                </p>
                <br/>
                <br/>
                <br/>   
                <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                <div><span style='margin: 0;'>Administrator</span></div>
              </body>
            </html>
            """
            mail.send(data['email'],'Registration Account',html)
        response = {'message':'Success','status':True}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["in"]
    if "list" in query :
        list_id = []
        for i in query["list"]:
            try:
                list_id.append(ObjectId(i))
            except Exception as e:
                print("failed")
        del query['list']
        query["_id"] = {
            "$in":list_id
        }
    sys.stdout.flush()
    result = userController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class count(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]

    result = userController.find(query)
    #sys.stdout.flush()
    if not result['status']:
        response = {"status":True, "message":"Data Not Found",'data':0}               
    else:
        response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class detail(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"] 

    result = userController.findOne(query)
    # print(result)
    # print("------------------")
    # sys.stdout.flush()

    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class login(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'password' not in data:
        response = {"status":False, "message":"Password Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    # if cloud9Lib.validEmail(data['username']):
    #     query = {'email':data['username']}
    # else :
    #     query = {'username':data['username']}

    query = {'email':data['email']}
    result = userController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
    else:
        password_db = cloud9Lib.decrypt(result['data']['password'])
        print(password_db)
        password = data['password']
        if password == password_db:
            if result['data']['active'] == False:
                response = {"status":False, 'message':'Account not active','data':json.loads(self.request.body)}
            else :
                response = {"status":True, 'message':'Login Success','data':result['data']}
        else:
            response = {"status":False, 'message':'Wrong password','data':json.loads(self.request.body)}
    
    self.write(response)

class activation(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'otp' not in data:
        response = {"status":False, "message":"OTP Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'sendlink' not in data:
        data['expired_otp'] = datetime.utcnow()
    else:
        del data['sendlink']

    query = data
    print(query)
    print("------------------")
    sys.stdout.flush()
    result = userController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"OTP Invalid, Try again",'data':json.loads(self.request.body)}               
    else:
        query = {"_id":ObjectId(result['data']["id"])}
        data = {"active":True,"otp":None}
        update = userController.update(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to activation","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Activation Success'}

    self.write(response)

class forgetpassword(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'otp' not in data:
        otp = cloud9Lib.randomString(6)
        expired_otp = datetime.utcnow() + timedelta(minutes=5)
        update = userController.update({'email':data['email']},{"otp":otp,"expired_otp":expired_otp})
        if not update['status']:
            response = {"status":False, "message":"Failed to request reset password","data":json.loads(self.request.body)}
        else:
            html = """\
            <html>
              <body>
                <p>Thank for using  our service,<br>
                   This is your otp to reset your password account?<br>
                </p> 
                <p>
                    OTP : <b>"""+otp+ """</b>
                </p>
                <br/>
                <br/>
                <br/>   
                <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                <div><span style='margin: 0;'>Administrator</span></div>
              </body>
            </html>
            """
            if 'link' in data:
                email = data['email']
                email = email.replace("@","518ed29525738cebdac49c49e60ea9d3",1)
                link = data['link']+email+"/"+otp
                html = """\
                <html>
                  <body>
                    <p>Thank for register in our service,<br>
                       This is your activation link?<br>
                    </p> 
                    <p>
                        Reset Password Link : <b>"""+link+ """</b>
                    </p>
                    <br/>
                    <br/>
                    <br/>   
                    <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                    <div><span style='margin: 0;'>Administrator</span></div>
                  </body>
                </html>
                """


            mail.send(data['email'],'Request Reset Password',html)
            response = {"status":True, 'message':'Request reset password Success'}
    else:
        if 'password' not in data:
            password = cloud9Lib.randomString()
            nopass = True
        else:
            password = data['password']
            del data['password']
            nopass = False
       
        data['expired_otp'] = datetime.utcnow()
        query = data
        result = userController.findOne(query)
        if not result['status']:
            response = {"status":False, "message":"OTP Invalid, Try again",'data':json.loads(self.request.body)}               
        else: 
            query = {"_id":ObjectId(result['data']["id"])}
            data = {"password":cloud9Lib.encrypt(password),"otp":None}
            update = userController.update(query,data)
            if not update['status']:
                response = {"status":False, "message":"Failed to reset password","data":json.loads(self.request.body)}
            else:
                if nopass == True:
                    response = {"status":True, 'message':'Reset Password Success', "data":{"password":password} }
                else:
                    response = {"status":True, 'message':'Reset Password Success' }

    self.write(response)

class update(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = userController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = userController.update(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to update","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Update Success'}
    self.write(response)

class delete(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    query = {"_id":ObjectId(data["id"])}
    result = userController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = userController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class changepass(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'password' not in data:
        response = {"status":False, "message":"Password Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'oldpassword' not in data:
        response = {"status":False, "message":"Old Password Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    query = {"_id":ObjectId(data["id"])}
    result = userController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
    else:
        password_db = cloud9Lib.decrypt(result['data']['password'])
        password_old = data['oldpassword']
        if password_old == password_db:
            update_data = {"password":cloud9Lib.encrypt(data['password'])}
            update = userController.update(query,update_data)
            if not update['status']:
                response = {"status":False, 'message':'Update password failed','data':json.loads(self.request.body)}
            else :
                response = {"status":True, 'message':'Update password success','data':json.loads(self.request.body)}
        else:
            response = {"status":False, 'message':'Wrong password','data':json.loads(self.request.body)}
    
    self.write(response)
