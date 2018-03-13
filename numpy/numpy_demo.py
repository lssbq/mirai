import numpy as np

print_ = print

def print(data):
    print_(data, end='\n----------------\n')

# Numpy basic: Array
def run_1():
    # Create ndarray, with dtype=float64
    arr = np.array([[1,1,1],
                    [2,0,2]], dtype=np.float64)
    print(arr)
    # Print basic info of array
    print('Array dim: %s' % arr.ndim)       # Dimensions of an ndarray
    print('Array type: %s' % arr.dtype)     # Type of an ndarray
    print('Array shae: %s' % str(arr.shape))# Shape of an ndarray
    print('Array size: %s' % arr.size)      # Elements of an ndarray, the product of 'shape'
    print('Array bytes: %s' % arr.itemsize) # Byte size of each element, 8 = 64/8

    print('='*20)

    # Create ndarray full of zero or one, default dtype=float64
    arr_zero = np.zeros((4, 4), dtype=np.int32)
    print(arr_zero, end='\n\n')

    arr_one = np.ones((4, 4), dtype=np.int16)
    print(arr_one, end='\n\n')

    # Create ndarray randomly or depends on memory
    arr_emp = np.empty((4, 4))
    print(arr_emp, end='\n\n')

    print('='*20)

    # Create sequence numbers
    # Numpy arange, like range() in Py
    arr_rag = np.arange(start=1, stop=21, step=2, dtype=np.float)
    print(arr_rag)

    # Get high precision float number
    arr_space = np.linspace(start=1, stop=5, num=50, endpoint=True).reshape(10, 5)
    print(arr_space)

# Basic operations
def run_2():
    arr_1 = np.array([[1,2,3],[3,2,1],[1,1,1]], dtype=np.int32)
    arr_2 = np.zeros((3,3), dtype=np.int32)
    print(arr_1)
    print(arr_1 < 3)

    # 元素乘
    print(arr_1 * arr_2)
    # 矩阵乘
    print(arr_1.dot(arr_2))
    print(arr_1)

    # 矩阵转置
    print(arr_1.T)

    # 矩阵所有元素求和
    print(arr_1.sum())
    # 矩阵所有元素中最小元素
    print(arr_1.min())
    # 矩阵所有元素中最大元素
    print(arr_1.max())
    # axis 参数指定按行(0)或列(1)操作
    print(arr_1.sum(axis=0))

    # 累加和(按列)
    print(arr_1.cumsum(axis=1))

# Index, Slicing, Iterating, Stack
def run_3():
    a = np.array([[1,2,3],[3,2,1],[1,1,1]], dtype=np.int32)
    print(a[0][0] == a[0, 0])

    # Python style slicing
    #[1, 2
    # 3, 2
    # 1, 1]
    print(a[0:3:1, 0:2])

    # 多维数组切片
    b = np.arange(12).reshape(2,2,3)
    print(b[:, :, 2])
    # ... 可以代替多余的冒号表示前面所有维度均不做切分
    print(b[..., 2])

    # Iterator of an Array
    for item in b.flat:
        print(item, end=' ')

# 合并矩阵 Stack
def run_4():
    part1 = np.array([1,1,1], dtype=np.int32)
    part2 = np.array([6,6,6], dtype=np.int32)

    # Vertical stack : 垂直合并--上下
    vstack = np.vstack((part1,part2))
    print(vstack)
    print(vstack.dtype)  # dtype会自动类型提升

    # Horizontal stack : 水平合并--左右
    hstack = np.hstack((part1, part2))
    print(hstack)
    print(hstack.shape)

    # 为一维矩阵创建新维度，矩阵转置不能提升数组维度
    arr = np.array([1,2,3])
    # 添加列维度
    # [[1]
    #  [2]
    #  [3]]
    print(arr[:, np.newaxis])

    # 一维数组按*元素*合并为二维数组 column_stack
    print(np.column_stack((part1, part2)))
    # Equivalent hstack + newaxis
    print(np.hstack((part1[:, np.newaxis], part2[:, np.newaxis])))

# 分割矩阵
def run_5():
    arr = np.arange(12).reshape(3, 4)
    print(arr)

    # Note: 分割方法是 Numpy 库方法
    # 纵向分割 hsplit
    print(np.hsplit(arr, 4))
    # 横向分割 vsplit
    print(np.vsplit(arr, 3))

    # 矩阵非对称分割 array_split
    print(np.array_split(arr, 3, axis=1))

    # Reshape如果其中一维度指定为-1，则另一个维度是自动计算的
    print(np.arange(12).reshape(2, -1))

# View and Copy
def run_6():
    a = np.arange(1, 13).reshape(3, -1)
    print(a)

    # 创建一个视图, 与a共享数据
    b = a.view()
    b.flags.owndata == False

    # 视图的形状可以改变, 但是a原始矩阵不变
    b.shape = 2,6
    print(b)

    # 改变任意值双发均发生改变
    a[0, 0] = 11
    print(b)

    # Deep Copy 创建深拷贝
    c = a.copy()
    print(c)

# Advance index
def index():
    # Create array of 0-11 square number
    arr = np.arange(12) ** 2
    print(arr)

    # Index by another array
    print(arr[np.array([1,3,5,7])])
    # Index by array array: 返回二维矩阵
    j = np.array([[2,4], [6,8]])
    print(arr[j])

    # 使用二维矩阵选择
    # i axis = 0
    i = np.array([[0,1],
                  [1,2]])
    # j axis = 1
    j = np.array([[2,1],
                  [3,3]])
    arr_2 = arr.view()
    arr_2.shape = 3,4
    print(arr_2[i, j])

    # Index of the maxima for each series
    print(arr_2.argmax(axis=1))

    # Index by boolean
    # 对 ndarray 进行比较操作可以返回含有布尔值的矩阵
    print(arr_2 > 9)
    print(arr_2[arr_2 >= 0])



if __name__ == '__main__':
    # run_1()
    # run_2()
    # run_3()
    # run_4()
    # run_5()
    # run_6()

    index()