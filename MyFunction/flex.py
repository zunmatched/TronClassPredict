import json, re, random
import pandas as pd

def flex_tcp_menu(index, message):
    output = {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "text",
                    "text": message,
                    "wrap": True
                }
            ]
        },
        "footer": {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "方案一",
                        "data": "action=tcp_why&index={}&plan=1".format(index)
                    }
                },
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "方案二",
                        "data": "action=tcp_why&index={}&plan=2".format(index)
                    }
                }, 
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "方案三",
                        "data": "action=tcp_why&index={}&plan=3".format(index)
                    }
                }
            ]
        }
    }
    return json.dumps(output)
    pass

