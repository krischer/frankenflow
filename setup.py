from setuptools import setup

setup(
    name="frankenflow",
    version="0.1",
    py_modules=["frankenflow"],
    install_requires=[
    ],
    entry_points="""
        [console_scripts]
        agere=ses3d_ctrl.ses3d_ctrl:cli
    """
)
