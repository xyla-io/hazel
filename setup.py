from setuptools import setup, find_packages

setup(name='hazel',
      version='0.0.1',
      description='Xyla\'s Python Google UAC client wrapper.',
      url='https://github.com/xyla-io/Hazel',
      author='Gregory Klein',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        "googleads",
        "pandas",
        "google-ads",
      ],
      zip_safe=False)
