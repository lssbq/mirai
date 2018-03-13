import numpy as np
import matplotlib.pyplot as plt

def mandelbrot(h, w, maxit=20):
    """ Return an image of the Mandelbrot fractal of size """
    y, x = np.ogrid[-1.4: 1.4: h*1j, -2: 0.8: w*1j]
    c = x + y*1j
    z = c
    divtime = maxit + np.zeros(z.shape, dtype=int)

    for i in range(maxit):
        z = z**2 + c
        diverge = z * np.conj(z) > 2 ** 2
        div_now = diverge & (divtime == maxit)
        divtime[div_now] = i
        z[diverge] = 2

    return divtime

plt.imshow(mandelbrot(800, 800))
plt.show()