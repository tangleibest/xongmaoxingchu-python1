import flask
import os
import sys
import json
from flask import request
from flask import Response
import xlwt
import pymysql
interface_path = os.path.dirname(__file__)
sys.path.insert(0, interface_path)  # 将当前文件的父目录加入临时系统变量

app = flask.Flask(__name__)

def set_style(font_name,font_colour,font_size,font_bold,underline,pattern_colour,ali_horz,ali_vert,left,right,top,bottom,italic):
    # 设置背景颜色
    pattern = xlwt.Pattern()
    # 设置背景颜色的模式
    pattern.pattern = xlwt.Pattern.SOLID_PATTERN
    # 背景颜色
    pattern.pattern_fore_colour = int(pattern_colour)
    font = xlwt.Font()
    # 字体类型
    font.name = font_name
    # 字体颜色
    font.colour_index = int(font_colour)
    # 字体大小，11为字号，20为衡量单位
    font.height = 20 * int(font_size)
    font.bold = font_bold
    #斜体
    font.italic = italic
    # 下划线
    font.underline = underline
    # 设置单元格对齐方式
    alignment = xlwt.Alignment()
    # 0x01(左端对齐)、0x02(水平方向上居中对齐)、0x03(右端对齐)
    alignment.horz = ali_horz
    # 0x00(上端对齐)、 0x01(垂直方向上居中对齐)、0x02(底端对齐)
    alignment.vert = ali_vert

    borders = xlwt.Borders()
    # 细实线:1，小粗实线:2，细虚线:3，中细虚线:4，大粗实线:5，双线:6，细点虚线:7
    # 大粗虚线:8，细点划线:9，粗点划线:10，细双点划线:11，粗双点划线:12，斜点划线:13
    borders.left = left
    borders.right = right
    borders.top = top
    borders.bottom = bottom

    # 初始化样式
    style0 = xlwt.XFStyle()
    style0.font = font
    style0.pattern = pattern
    style0.alignment = alignment
    style0.borders = borders
    return style0

def file_iterator(file_path, chunk_size=512):
    """
        文件读取迭代器
    :param file_path:文件路径
    :param chunk_size: 每次读取流大小
    :return:
    """
    with open(file_path, 'rb') as target_file:
        while True:
            chunk = target_file.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break


def to_json(obj):
    """
        放置
    :return:
    """
    return json.dumps(obj, ensure_ascii=False)


# 下载
@app.route('/download', methods=['GET'])
def download():
    """
        文件下载
    :return:
    """

    # file_path = request.values.get('filepath')
    file_path='C:\\Users\\tl\\PycharmProjects\\get_excel\\finance\\a-1574760862.xlsx'
    print(file_path)
    if file_path is None:
        return to_json({'success': 0, 'message': '请输入参数'})
    else:
        if file_path == '':
            return to_json({'success': 0, 'message': '请输入正确路径'})
        else:
            if not os.path.isfile(file_path):
                return to_json({'success': 0, 'message': '文件路径不存在'})
            else:
                filename = os.path.basename(file_path)
                response = Response(file_iterator(file_path))
                response.headers['Content-Type'] = 'application/octet-stream'
                response.headers["Content-Disposition"] = 'attachment;filename="{}"'.format(filename)
                return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000,debug=True)