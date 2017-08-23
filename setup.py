from setuptools import setup

setup(name='gydelt',
      version='1.0',
      description='Collect, clean, pre-process and store data from GDELT easily',
      url='',
      author='Mrinal Jain',
      author_email='mrinaljain007@gmail.com',
      license='MIT',
      packages=['gydelt'],
	  install_requires=['pandas', 'pandas_gbq', 'google-cloud-bigquery', ],
	  classifiers = [
		'Development Status :: 3 - Alpha',
		'Environment :: Console',
		'Intended Audience :: Developers',
		'Programming Language :: Python :: 3.6',
		'Topic :: Scientific/Engineering',
		'Topic :: Software Development :: Version Control :: Git'
	  ],
      zip_safe=False)