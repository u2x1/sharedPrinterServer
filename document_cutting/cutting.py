import numpy as np
import cv2 as cv

def read_img(path, mode):
    """
    输入：图片文件路径
    处理：读取图片，将图片转化为灰阶通道，等比例缩放为height=480
    返回：图片矩阵
    """
    img = cv.imread(path, mode)
    if mode == 0:
        width, height = (int(img.shape[1] * (480/img.shape[0])), int(img.shape[0] * (480/img.shape[0])))
        img = cv.resize(img, (width, height), interpolation = cv.INTER_AREA)
    else: 
        width, height = (img.shape[1], img.shape[0])
    # print(img.shape)
    return img, width, height

def extract_points(img):
    """
    输入：numpy格式的图片
    输出：图片中卡片的四个顶点相对于图片的坐标
    """
    blur_img = cv.GaussianBlur(img, (5,5), 0)
    kernel = cv.getStructuringElement(cv.MORPH_ELLIPSE, (5,5))
    morph_img = cv.morphologyEx(blur_img, cv.MORPH_CLOSE, kernel)

    canny_img = cv.Canny(morph_img, 40, 80)
    kernel = cv.getStructuringElement(cv.MORPH_ELLIPSE, (5,5))
    canny_img = cv.dilate(canny_img,kernel,iterations = 1)
    
    contours, hierarchy = cv.findContours(canny_img, cv.RETR_TREE, cv.CHAIN_APPROX_SIMPLE)
    contour_img = np.full(img.shape, 0, dtype=np.uint8)
    maxArea = 0
    for i in range(len(contours)):
        area = cv.contourArea(contours[i])
        if area > maxArea:
            maxArea = area
            maxEdgeIndex = i
            # print(maxArea)
    cv.drawContours(contour_img, contours, maxEdgeIndex, (255), 1)

    gray = np.float32(contour_img)
    dst = cv.cornerHarris(gray,2,3,0.04)

    y, x = np.where(dst > 0.01*dst.max())
    points = np.c_[x,y]
    return points

def get_points_order(cross_points):
    """
    输入：四个点的(x, y)值
    处理：使用动态规划求四个角的顺序
    输出：左上，右上，右下，左下
    """
    
    top_left_point = ()
    top_right_point = ()
    buttom_left_point = ()
    buttom_right_point = ()
    # x+y
    max1 = 0
    min1 = 0
    # y-x
    max2 = 0
    min2 = 0
    for i in range(len(cross_points)):
        # x+y最大者为右下角，最小者为左上角
        # y-x最大者为左下角，最小者为右上角
        x_plus_y = cross_points[i][0] + cross_points[i][1]
        y_minus_x =  cross_points[i][1] - cross_points[i][0]
        if i == 0:
            top_left_point = cross_points[i]
            top_right_point = cross_points[i]
            buttom_left_point = cross_points[i]
            buttom_right_point = cross_points[i]
            max1 = x_plus_y
            max2 = y_minus_x
            min1 = x_plus_y
            min2 = y_minus_x
        else:
            if x_plus_y > max1:
                max1 = x_plus_y
                buttom_right_point = cross_points[i]
            if y_minus_x > max2:
                max2 = y_minus_x
                buttom_left_point = cross_points[i]
            if x_plus_y < min1:
                min1 = x_plus_y
                top_left_point = cross_points[i]
            if y_minus_x < min2:
                min2 = y_minus_x
                top_right_point = cross_points[i]

    points = np.array([
        top_left_point, 
        top_right_point, 
        buttom_right_point, 
        buttom_left_point
        ], np.float32)

    return points

def perspective_transform(img, points, size, spin=False):
    # print(img.shape)
    # print(points)
    # print(size)
    dst_width = size[0]
    dst_height = size[1]
    if not spin:
        dst_points = np.float32([
            [0, 0],
            [dst_width,0],
            [dst_width,dst_height],
            [0,dst_height],
            ])
    else:
        tmp = dst_height
        dst_height = dst_width
        dst_width = tmp
        dst_points = np.float32([
            [0, dst_height],
            [0,0],
            [dst_width,0],
            [dst_width,dst_height],
            ])
    perspective_Mat = cv.getPerspectiveTransform(points, dst_points)
    dst_img = cv.warpPerspective(img, perspective_Mat, (int(dst_width), int(dst_height)))
    return dst_img

def get_card_position(image_path):
    img_gray, width, height = read_img(image_path, 0)
    points = extract_points(img_gray)
    points = get_points_order(points)
    position = {
        "a": {"x": points[0][0] / width, "y": points[0][1] / height},
        "b": {"x": points[1][0] / width, "y": points[1][1] / height},
        "c": {"x": points[2][0] / width, "y": points[2][1] / height},
        "d": {"x": points[3][0] / width, "y": points[3][1] / height},
    }
    return position

def cut(image_path, position, target_size=None):
    img_src, width, height = read_img(image_path, -1)
    points = np.float32([
        [position['a']['x'] * width, position['a']['y'] * height],
        [position['b']['x'] * width, position['b']['y'] * height],
        [position['c']['x'] * width, position['c']['y'] * height],
        [position['d']['x'] * width, position['d']['y'] * height],
    ])
    # print(target_size)
    spin = False
    if not target_size:
        max_width = max(abs(points[0][0]-points[1][0]), abs(points[2][0]-points[3][0]))
        max_height = max(abs(points[0][1]-points[3][1]), abs(points[1][1]-points[2][0]))
        target_size = (max_width, max_height)
    elif target_size[0] < target_size[1]:
        spin = True
    dst_img = perspective_transform(img_src, points, target_size, spin)
    return dst_img

if __name__ == "__main__":
    position = get_card_position(r'test7.jpg')
    cut('test7.jpg', position, (210,297))