"""
Goal: This script contains the very basic logger which can be reused across the project.

Created by: Nishesh Kalakheti
Created on: 2nd Feb, 2022
"""

import logging

logger = logging
logger.basicConfig(filename='assessment.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
