# import json

join_operator_list = ["and","or"]
# rules_data=["ch_1","ch_2","ch_3"]
# rules_json={
#     "rules":{
#         "1":["_ch_1_",">=","90"], #data,#operator,#operands
#         "2":["_ch_2_",">=","70"],
#         "3":["_ch_3_","<=","20"],
#     },
#     "join":["rules1","and","rules2","and","rules3"]
# }

# sensor_data={
#     "ch_1":90,
#     "ch_2":90,
#     "ch_3":23
# }

# def converter(rules):
#     rules_string=""
#     rules_list = rules["rules"]
#     rules_string_list={}
#     for i in rules_list:
#         rules_string_list["rules"+i] = str(rules_list[i][0])+" "+str(rules_list[i][1])+" "+str(rules_list[i][2])

#     rules_join = rules["join"]
#     for item in rules_join:
#         if(item not in join_operator_list):
#             rules_string+=" ("+rules_string_list[item]+") "
#         else:
#             rules_string+=" "+item+" "
#     return rules_string

# def evaluation(sensor_data,rules_data,rules_string):
#     params_string = ""
#     for item in rules_data:
#         if( item in sensor_data):
#             value = sensor_data[item]
#         else:
#             value = 0
#         params_string+= "_"+item+"_ = "+str(value)+"\n"
    
#     exec(params_string)
#     return eval(rules_string)

# rules_string = converter(rules_json)
# result = evaluation(sensor_data,rules_data,rules_string)

# print(result)


#print( ( _ch_1_ >= 90) and ( _ch_2_ >= 90) and ( _ch_3_ <= 10) )

script = " (_ch_1_ >= 90)  and  (_ch_2_ >= 70)  and  (_ch_3_ <= 20) "


def item_deconverter(script):
    script.replace("(", "").replace(")", "")
    script_item = script.split(" ")
    return script_item

def deconverter(script):
    script_join = script.split("  ")
    rules_list = {}
    join_list = [] 
    rules_number = 1
    for item in script_join:
        item = str(item).strip()
        if( item in  join_operator_list):
            join_list.append(item)
        else:
            join_list.append("rules"+str(rules_number))
            rules_list[str(rules_number)] = item_deconverter(item)
            rules_number +=1

    
print(rules_list)
print(join_list)