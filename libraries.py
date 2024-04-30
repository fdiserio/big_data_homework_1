import streamlit as st
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import requests
from bs4 import BeautifulSoup