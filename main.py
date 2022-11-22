
import os
import re
import json
import cv2
import numpy
import pdfplumber
from random import randint
from queue import Queue
from threading import Thread
from pdfminer.pdfparser import PDFSyntaxError
from flask import Flask, request, jsonify, send_from_directory, make_response

from mytool.Order import Order
from mytool.File import File
from DbClient.DbClient import DbClient
from mytool.alivePrinter import AlivePrinters
from document_cutting import cutting

# from websocket_server.WebsocketServer import WsServer

# file_directory = "orderFiles/"
# idcard_directory = "idCard/"
# db = DbClient("localhost","root","58251190","printer")

file_directory = "/home/admin/flask_uwsgi/myserver/orderFiles/"
idcard_directory = "/home/admin/flask_uwsgi/myserver/idCard/"
db = DbClient("localhost","root","58251190abcdshe","printer")

orderList = Queue(maxsize=0)
# alivePrinter = AlivePrinters(db)
app = Flask(__name__)
# wsServer = WsServer(orderList, db)

@app.route('/wwkserver/')
def hello_world():
    r = request.args.get('info')
    if r==None:
        return 'args error'
    return r

# --------------------------------------------通常为小程序使用------------------------------------

def check_token(openid):
    ret = db.openid(openid)
    if ret:
        return True
    else:
        return False

@app.route('/wwkserver/ip/')
def getip():
    print({_[0]: _[1] for _ in list(request.headers)}.get('X-Forwarded-For'))
    if request.headers.get('X-Forwarded-For'):
        ip = request.headers['X-Forwarded-For']
    elif request.headers.get('X-Real-IP'):
        ip = request.headers.get('X-Real-IP')
    else:
        ip = request.remote_addr
    db.writeIP(ip)
    return "ok"

@app.route('/wwkserver/printer/')
def getPrinter():
    printer_id = request.args.get('printerid')
    if printer_id==None:
        ret = db.getPrinterList()
        return jsonify(ret)
    else:
        ret = db.get_printer_info(printer_id)
        return jsonify(ret)

# 创建一个新订单
@app.route('/wwkserver/addorder/', methods=['POST'])
def addorder():
    form = json.loads(request.data)

    printer_id = form["printer_id"]
    openid = form["openid"]
    order = Order(
        printer_id = form["printer_id"],
        openid = form["openid"],
        status = False,
        )
    order_id = db.addOrder(order)
    order.order_id = order_id
    orderList.put(order)
    return {
        "status": "ok",
        "order_id": order_id
    }

def get_pdf_page(pdf_path):
    try:
        page_direction = "portrait"
        pdf = pdfplumber.open(pdf_path)
        if pdf.pages[0].width > pdf.pages[0].height:
            page_direction = "landscape"
        else:
            page_direction = "portrait"
        page_num = len(pdf.pages)
        black_rate_list = []
        if page_num > 10:
            for _ in range(3):
                # 随机抽三页
                page = randint(0, page_num-1)
                img = pdf.pages[page].to_image()
                tmp_img_file = open("temp", "wb")
                img.save(tmp_img_file, "PNG")
                tmp_img = cv2.imread("temp", 0)
                even_value = numpy.sum(tmp_img)/tmp_img.size
                black_rate = 1 - even_value/255
                black_rate_list.append(black_rate)
            if black_rate_list[0]>0.3 and black_rate_list[1]>0.3 and black_rate_list[2]>0.3:
                # 黑色区域过多
                if black_rate_list[0]==black_rate_list[1] and black_rate_list[2]==black_rate_list[1]:
                    # 且页面重复
                    return False, page_direction
                else: 
                    # 再抽五页
                    for _ in range(3):
                        page = randint(0, page_num-1)
                        img = pdf.pages[page].to_image()
                        tmp_img_file = open("temp", "wb")
                        img.save(tmp_img_file, "PNG")
                        tmp_img = cv2.imread("temp", 0)
                        even_value = numpy.sum(tmp_img)/tmp_img.size
                        black_rate = 1 - even_value/255
                        black_rate_list.append(black_rate)
                    if black_rate_list[3]>0.3 and black_rate_list[4]>0.3 and black_rate_list[5]>0.3:
                        return False, page_direction
    except Exception as e:
        print(e)
        page_num = 0
    return page_num, page_direction

@app.route('/wwkserver/addorder/uploadfile', methods=['POST'])
def uploadfile():
    form = request.form

    storage_name = form["storage_name"]
    docType = re.search("\.(.+)", storage_name).group(1)
    my_file = request.files["file"]
    my_file.save(f"{file_directory}{storage_name}")
    page_direction = ""
    if docType == "pdf":
        # pdf
        page_num, page_direction = get_pdf_page(f"{file_directory}{storage_name}")
    elif docType not in ["png", "jpg", "jpeg"]:
        # word等文档
        with os.popen(f"unoconv -f pdf {file_directory}{storage_name}") as ret:
            print(ret)
        storage_name = re.sub("\..+", ".pdf", storage_name)
        page_num, page_direction = get_pdf_page(f"{file_directory}{storage_name}")
    else: 
        # 图片
        page_num = 1
    print(f"storage_name={storage_name}, page_num={page_num}")
    # print(f"orderid={orderid},storage_name={storage_name}.{docType}")
    # if docType == "pdf":
    #     page_num = get_pdf_page(f"{file_directory}{storage_name}.{docType}")
    #     db.setPage_num(orderid, page_num)
    # db.setStorage_name(orderid, f"{storage_name}.{docType}")
    file = File(
        openid = form["openid"],
        storage_name = storage_name,
        file_name = form["file_name"],
        file_size = form["file_size"],
        order_id = form["order_id"],
        upload_time = form.get("upload_time"),
        page_num = page_num,
        page_direction = form.get("page_direction"),
        file_type = form.get("file_type"),
        status = False
        )
    file_id = db.addFile(file)
    response = make_response({
            "page_num": page_num,
            "file_id": file_id,
            "storage_name": storage_name,
            "page_direction": page_direction,
            })
    headers = {
        'content-type':'application/json',
        "Access-Control-Allow-Origin": "*"
    }
    response.headers = headers
    return response

@app.route('/wwkserver/addorder/submitorder', methods=['POST'])
def submitorder():
    form = json.loads(request.data)
    file_list = json.loads(form['fileList'])
    order_fee = int(form['fee'])
    order_id = int(form['order_id'])
    is_ack = int(form['is_ack'])
    file_num = len(file_list)
    db.submitorder(order_id, is_ack, file_num)
    db.update_order_fee(order_fee, order_id)
    for i in file_list:
        start = i["page_range_start"] if "page_range_start" in i else 1
        end = i["page_range_end"] if "page_range_end" in i else i["page_num"]
        range = ''
        range += str(start) if start else '1'
        range += '-'
        range += str(end) if end else i["page_num"]
        print(start)
        print(end)

        file = File(
            file_id = i['file_id'],
            is_duplex = i["is_duplex"],
            page_range = range,
            page_direction = i["page_direction"],
            copy_num = i["copy_num"],
            status = False
            )
        db.updateFileInfo(file)
    return {
        "status": "ok"
    }

@app.route('/wwkserver/checkorderstatus/')
def checkOrderStatus():
    printer_id = request.args.get('printer_id')
    order_id = int(request.args.get("order_id"))
    if printer_id == None:
        return 'args error'
    else:
        ret = db.getOrderByPrinter(printer_id)
        rank = 0
        status = True
        for i in range(len(ret)):
            if ret[i].order_id == order_id:
                rank = i
                status = ret[i].status
                break
        return {
            "order_id": order_id,
            "rank": rank,
            "status": status,
        }

@app.route('/wwkserver/pay/')
def pay():
    orderid = int(request.args.get("orderid"))
    if db.ispay(orderid):
        return "ok"
    else: 
        return "error"

@app.route('/wwkserver/ackprinting/')
def ack_printing():
    orderid = int(request.args.get("orderid"))
    if db.ack_printing(orderid):
        return "ok"
    else: 
        return "error"

@app.route('/wwkserver/history/', methods=['POST'])
def get_history():
    form = json.loads(request.data)
    openid = form['openid']
    start = int(form['start'])
    count = int(form['count'])
    if not openid:
        return "arg error"
    else: 
        order_list = db.get_user_orders(openid, start, count)
        return jsonify([order.getOrderString() for order in order_list])

@app.route('/wwkserver/lastorder/')
def lastorder():
    openid = request.args.get("openid")
    order = db.get_user_orders(openid, 0, 1)[0]
    if order:
        if not order.status:
            # 如果订单未完成
            return order.getOrderString()
    return "0"

@app.route('/wwkserver/cardposition/', methods=['POST'])
def get_card_position():
    form = request.form
    openid = form.get("openid")
    file_name = form.get("file_name")
    if not (openid or file_name):
        return "error"
    # print(openid, file_name)

    card = request.files["file"]
    print(f"{idcard_directory}{file_name}")
    card.save(f"{idcard_directory}{file_name}")

    position = cutting.get_card_position(f"{idcard_directory}{file_name}")
    file_id = db.use_card(openid, file_name)
    return {
        "position": position,
        "id": file_id
    }

@app.route('/wwkserver/transform/', methods=['POST'])
def transform():
    form = json.loads(request.data)
    openid = form.get("openid")
    file_name = form.get("file_name")
    position = form.get("position")
    size = form.get("size")
    file_id = form.get("file_id")
    if not (openid or file_name or position or size or file_id):
        return "error"

    dst_img = cutting.cut(f"{idcard_directory}{file_name}", position, size)
    cv2.imwrite(f"{idcard_directory}extracted-{file_name}", dst_img)

    # response = make_response(
    #     send_from_directory(idcard_directory, f"extracted-{file_name}", as_attachment=True))
    db.update_position(position, file_id)
    return "ok"

@app.route('/wwkserver/getidcard/', methods=['GET'])
def getincard():
    openid = request.args.get("openid")
    file_id = request.args.get("file_id")
    file_name = request.args.get("file_name")
    if not (openid or file_id or file_name):
        return "error"

    # print(openid, file_id, file_name)

    ret = db.read_card(openid, file_id, file_name)
    print(ret)

    response = make_response(
        send_from_directory(idcard_directory, f"extracted-{file_name}", as_attachment=True))
    return response
# ---------------------------------------------通常为终端使用--------------------------------------

@app.route('/wwkserver/checkmyorder/', methods=['GET', 'POST'])
def checkOrderByPrinterID():
    printerid = request.args.get('printerid')
    # print(printerid, request.method)
    # alivePrinter.keepAlive(printerid)
    if printerid==None:
        return 'args error'
    ret = db.getOrderByPrinter(printerid)

    if request.method=='GET':
        orders = request.args.getlist('orders')
    if request.method=='POST':
        orders = request.form.getlist('orders')

    if orders:
        print(orders)
        remove_list = []
        for order in ret:
            for order_id in orders:
                if order.order_id == int(order_id):
                    remove_list.append(order)
        for order in remove_list:
            ret.remove(order)
    return jsonify([oneOrder.getOrderString() for oneOrder in ret])

@app.route('/wwkserver/getorderfiles/')
def getFilesByOrderId():
    orderid = request.args.get('orderid')
    if orderid==None:
        # do something
        return 'args error'
    else:
        files = db.get_files_by_order(orderid)
        # print(files)
        return jsonify([file.__dict__ for file in files])

@app.route('/wwkserver/getfiles/')
def get_file():
    file_name = request.args.get('filename')
    if file_name==None:
        # do something
        return 'args error'
    else:
        print(file_name)
        response = make_response(
            send_from_directory("/home/admin/flask_uwsgi/myserver/orderFiles", file_name, as_attachment=True))
        return response

@app.route('/wwkserver/fileok/')
def fileok():
    fileid = request.args.get('fileid')
    if fileid==None:
        # do something
        return 'args error'
    else:
        db.fileComplete(fileid)
        return "ok"

@app.route('/wwkserver/oddsideok/')
def oddSideOk():
    orderid = request.args.get('orderid')
    if orderid==None:
        # do something
        return 'args error'
    else:
        try:
            db.setPrintSide(orderid, "oddy")
            return "ok"
        except:
            return "error"

if __name__ == '__main__':
    Thread(target=app.run, args=['0.0.0.0',5000]).start()
    # page_num, page_direction = get_pdf_page("刷题表.pdf")
    # print(page_num, page_direction)
    # Thread(target=wsServer.run).start()
# <a style="color:#aaa;" href="https://beian.miit.gov.cn/" target="_blank">粤ICP备2021146850号-1</a>
