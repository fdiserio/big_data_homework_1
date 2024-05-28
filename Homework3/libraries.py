import streamlit as st
import pymongo
import pandas as pd
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
import re
import os
import requests
from bs4 import BeautifulSoup

uri_D = "mongodb+srv://Homework3:Homework3@clusterhomework3.1gev4ck.mongodb.net/?retryWrites=true&w=majority&appName=ClusterHomework3"
uri_F = "mongodb+srv://Homework3:Homework3@homework3.fc8huhi.mongodb.net/?retryWrites=true&w=majority&appName=Homework3"
uri = uri_F