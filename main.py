"""
Google Spreadsheet DHT Sensor Data-logging
NAS Data-logging
Local SD Card Data-logging
Logging anamolies
"""
# debug using print statements verbose
# verbose = True
# testing = True
verbose = False
testing = False

import os
import sys
import time
import datetime
import csv
import statistics
from itertools import combinations
from pathlib import Path
import Adafruit_DHT as af
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import logging.handlers as handlers


def bugprn(x=None, verbose=False):
    '''
        Printing statements for # DEBUG: following functions
        x = b(egin) or e(nd)
    '''
    msg = False
    if x == None or x == 'b' or x == 'begin' or x == 'start' or x == 's':
        msg = f'\n################ Running in function: {sys._getframe(0).f_code.co_name} ###############'
    else:
        msg = f'\t---------------- Returning to: {sys._getframe(1).f_code.co_name} ----------------------'
    print(msg)
    return msg


def login_open_sheet(d, oauth_file, spreadsheet, verbose=False):
    """ Connect to Google Docs spreadsheet and return the sheet1.
        d is the path dictionary with full path to log file.
        oauth_file points to authorize file from google:
            Check out google sheets api for setup specific a particular
            environment.  (This setup is for a project in google cloud
            providing access with credentials for google's service account
            for this project in google cloud)
        spreadsheet is the name of the google sheet.
    """
    if verbose: bugprn()
    msg = False
    try:  # prevent stopping with try/except if unable to open google sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(oauth_file, scope)
        gc = gspread.authorize(credentials)
        worksheet = gc.open(spreadsheet).sheet1
        msg = f'type(worksheet): {type(worksheet)}'
        if verbose: print(msg)
        if verbose: bugprn(x='e')
        return worksheet
    except Exception as Ex:  # prints
        msg = f'Error logging in to google sheets! Ex: {Ex}'
        print(msg)  # log from function in while loop
        worksheet=False
    if verbose: bugprn(x='e')
    return worksheet


def next_measurement(time_init, freq_min, verbose=False):
    ''' Set up to sleep for desired minutes.
    '''
    if verbose: bugprn()
    # Shows the sleep for time
    msg = f'\t\t\tSleep set for {freq_min * 60} seconds from {datetime.datetime.now().isoformat()}'
    if verbose: print(msg)
    time.sleep(freq_min * 60)
    if verbose: bugprn(x='e')
    return msg


def write_log(d, dir_dct, logger, txt, when='W6', interval=4, backupCount=5, verbose=False):
    ''' logger setup
        set for weekly starting on sunday ('W6') every
        (interval) 4 weeks backed up 5 (sets of 4 weeks approx: 4 months)
    '''
    if verbose: bugprn()
    msg = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_local = logger
    logger_share = logger

    pth_local = f'log/{d["file_log"]}'
    logHandler_local = handlers.TimedRotatingFileHandler(
        filename=pth_local, when=when, interval=interval, backupCount=backupCount)
    logHandler_local.setLevel(logging.INFO)
    logHandler_local.setFormatter(formatter)
    logger_local.addHandler(logHandler_local)
    logger_local.info(txt)
    time.sleep(1)

    pth_share = dir_dct['log_pth']
    logHandler_share = handlers.TimedRotatingFileHandler(
        filename=pth_share, when=when, interval=interval, backupCount=backupCount)
    logHandler_share.setLevel(logging.INFO)
    logHandler_share.setFormatter(formatter)
    logger_share.addHandler(logHandler_share)
    logger_share.info(txt)
    time.sleep(1)

    if verbose: bugprn(x='e')
    return


def get_avg(d, data, dir_dct, threshold_flg=False, verbose=False):
    ''' identify nonetype and out of range
        20-80% humidity readings with 5% accuracy
        0-50°C (32-122°F) temperature readings ±2°C accuracy
    '''
    if verbose: bugprn()
    msg = False
    d_i = valid_data(d=d, data=data, dir_dct=dir_dct, threshold_flg=False, verbose=verbose)
    msg = f'd_i:{d_i}'
    if verbose: print(msg)
    d_avg = ()
    for k, v in d_i.items():
        msg = f'k:{k}\tv:{v}\td_i.items:{d_i.items()}'
        if verbose: print(msg)
        s = 0
        if len(v) > 0:
            for i in range(len(v)):
                s = s+v[i]
            d_avg += (s/len(v),)
        else:
            d_avg += (None,)
    msg = f'd_avg {d_avg}'
    if verbose: print(msg)
    if verbose: bugprn(x='e')
    return d_avg


def valid_threshold(d, data, data_prev, dir_dct, verbose=False):
    ''' find if data is within threshold
        d is the datatype tuple.
        data_prev is a tuple of previously collected data
        dir_file is a complete path and file name
        threshold is the acceptable level of difference between data points
            as determined by min/max
    '''
    msg = False
    if verbose: bugprn()
    msg = f'd:{d}\ndata {data}\ndir_dct{dir_dct}'
    if verbose: print(msg)
    msg = f'threshold: {d["threshold"]}\tverbose:{verbose}'
    if verbose: print(msg)
    threshold = d["threshold"]
    valid = valid_data(d=d, data=data, dir_dct=dir_dct, threshold_flg=True, verbose=verbose)
    msg = f'threshold: {threshold}\tverbose:{verbose}\tvalid:{valid}\tdata:{data}'
    if verbose: print(msg)
    if isinstance(data, dict):
        msg = f'data.keys():{data.keys()}'
        if verbose: print(msg)
        pin = list(data.keys())[0]
        data = data[pin]
        msg = f'data:{data}'
        if verbose: print(msg)
    msg = f'valid:{valid}'
    if verbose: print(msg)
    if valid == False:
        if verbose: bugprn(x='e')
        return False
    else:
        if threshold is None:
            threshold = 0.90
        elif threshold < 0.5:
            threshold = 1 - threshold
        elif threshold >= 50:
            threshold = threshold/100
        elif threshold > 1:
            threshold = (100 - threshold)/100
        msg = f'Calc threshold: {threshold}\tlen(d[data]):{len(d["data"])}'
        if verbose: print(msg)
        for i in range(len(d['data'])):
            msg = f'data[i]:{data[i]}\tdata_prev[i]:{data_prev[i]}'
            if verbose: print(msg)
            if isinstance(data[i], (int, float)) and isinstance(data_prev[i], (int, float)):
                if max(data[i], data_prev[i]) > 0:
                    h = min(data[0], data_prev[0])/max(data[0], data_prev[0])
                    t = min(data[1], data_prev[1])/max(data[1], data_prev[1])
                else:
                    if verbose: bugprn(x='e')
                    return False
            else:
                if verbose: bugprn(x='e')
                return False
        msg = f'pin:{pin} h: {h}\tt: {t}\tthr: {threshold}'
        if verbose: print(msg)
        if h < threshold or t < threshold:
            msg = f'{threshold} threshold data breached: pin:{pin} h:{h} t:{t}'
            print(msg)
            if verbose: bugprn(x='e')
            return False
        if verbose:
            if verbose: bugprn(x='e')
        return True
    if verbose:
        if verbose: bugprn(x='e')
    return valid


def valid_data(d, data, dir_dct, threshold_flg=None, verbose=False):
    ''' Validate d provided.
        d is the datatype dictionary.
        dir_dct is a complete path and file name
        identify nonetype and out of range
        20-80% humidity readings with 5% accuracy
        0-50°C (32-122°F) temperature readings ±2°C accuracy
    '''
    if verbose: bugprn()
    msg = f'threshold_flg:{threshold_flg}'
    if verbose: print(msg)
    if isinstance(data, tuple):
        data = {0: data}
    ctr_i = -1
    d_i = {}
    for i in d['data']:
        ctr_i += 1
        d_i[i] = ()
        for k, v in data.items():
            msg = f'i:{i}\tctr_i:{ctr_i}\tk:{k}\tv:{v}\tdata[k][ctr_i]:{data[k][ctr_i]}'
            if verbose: print(msg)
            if isinstance(data[k][ctr_i], (int, float)):
                if i == 'Humidity' or i == d['data'][0]:
                    if data[k][ctr_i] > 20 and data[k][ctr_i] < 80:
                        d_i[i] += (data[k][ctr_i],)
                        msg = f'data[k][ctr_i]: {data[k][ctr_i]}\td_i[i]: {d_i[i]}'
                        if verbose: print(msg)
                    else:
                        # write to log file
                        msg = f'Not in humidity sensor range: pin:{k} {d["data"][ctr_i]}'
                        print(msg)
                        #write_log
                        if threshold_flg == True:
                            if verbose: bugprn(x='e')
                            return False
                if i == 'Temperature' or i == d['data'][1]:
                    if data[k][ctr_i] > 32 and data[k][ctr_i] < 122:
                        d_i[i] += (data[k][ctr_i],)
                        msg = f'data[k][ctr_i]: {data[k][ctr_i]}\td_i[i]: {d_i[i]}'
                        if verbose: print(msg)
                    else:
                        # write to log file
                        msg = f'not in temperature sensor range: pin:{k} {d["data"][ctr_i]}'
                        print(msg)
                        if threshold_flg == True:
                            if verbose: bugprn(x='e')
                            return False
            else:
                msg = f'not a number: pin:{k} {d["data"][ctr_i]}'
                print(msg)
                if threshold_flg == True:
                    # write to log file
                    if verbose: bugprn(x='e')
                    return False
    if threshold_flg == True:
        if verbose: bugprn(x='e')
        return True
    else:
        if verbose: bugprn(x='e')
        return d_i


def reading_sensor(d, pin, dir_dct, verbose=False):
    ''' for DHT11 sensor only
        read only one sensor at a time
    '''
    if verbose: bugprn()
    msg = False
    data = {}
    try:
        humidity, temperature_c = af.read_retry(d['instance'], pin)
        temperature_f = temperature_c * (9 / 5) + 32
        data[pin] = (humidity, temperature_f)
        msg = f'dp: {data[pin]}'
        if verbose: print(msg)
        time.sleep(1)
    except Exception as Ex:
        time.sleep(1)
        msg = f'dp reading error: {data[pin]}\tpin: {pin}\t Ex: {Ex}'
        if verbose: print(msg)
    if verbose: bugprn(x='e')
    return data[pin]


def reading_sensors(d, data_prev, dir_dct, verbose=False):
    ''' Reading data from a sensor (DHT11)
        d ist the sensor_dict data
        data_prev is a tuple of previously collected data
        dir_dct is directory of paths and file paths
    '''
    if verbose: bugprn()
    msg = False
    msg = f'data_prev:{data_prev}\ndir_dct{dir_dct}'
    if verbose: print(msg)
    msg = f'threshold: {d["threshold"]}\tverbose:{verbose}'
    if verbose: print(msg)
    # Initialize data
    data = {}
    data_test = {}
    msg = f'd[pins]:{d["pins"]}'
    if verbose: print(msg)
    for pin in d['pins']:
        ctr_pin = 0
        data_test[pin] = False
        msg = f'data_test[pin]:{data_test[pin]}'
        if verbose: print(msg)
        while ctr_pin < 5 and data_test[pin] == False:
            ctr_pin += 1
            data[pin] = reading_sensor(d=d, pin=pin, dir_dct=dir_dct, verbose=verbose)
            time.sleep(1)
            msg = f'ctr_pin{ctr_pin}\tdata[pin]{data[pin]}'
            if verbose: print(msg)
            data_test[pin] = valid_threshold(
                d=d, data={pin: data[pin]}, data_prev=data_prev, dir_dct=dir_dct, verbose=verbose)
            time.sleep(5)
    if verbose: bugprn(x='e')
    return data


def chk_mk_dir(dir, verbose=False):
    ''' Check if directory exists and make if not
    '''
    msg = False
    if verbose: bugprn()
    if not Path(dir).is_dir():
        try:
            os.mkdir(dir)
            msg = f'dir: {dir}'
            if verbose: print(msg)
            return dir
        except Exception as Ex:
            msg = f'not made dir: {dir} Ex: {Ex}'
            if verbose: print(msg)
    if not Path(dir).is_dir():
        try:
            os.system('sudo mkdir ' + dir)
            msg = f'dir system: {dir}'
            if verbose: print(msg)
            return dir
        except Exception as Ex:
            msg = f'no system dir: {dir} Ex:{Ex}'
            print(msg)
    if not Path(dir).is_dir():
        msg = f'\t{dir} could not be created!\n\tDefault to current directory'
        if verbose: print(msg)
        dir = os.getcwd()
        msg = f'curdir: {dir}'
        if verbose: print(msg)
    msg = f'Path {dir} is'
    if verbose: print(msg)
    if verbose: bugprn(x='e')
    return dir


def mnt_share(nm_prj, d, pth_share, verbose=False):
    ''' Mount shared folder return project path and paths to data and log files
    '''
    if verbose: bugprn()
    msg = False
    pth_prj = os.getcwd()
    try:
        if os.path.exists(pth_share) == False:
            cmd_mnt = 'sudo mount -t cifs -o username=pi,password=raspberrypi,uid=pi,gid=pi,forceuid,forcegid, //192.168.4.1/share /share'
            try:
                os.system("".join(cmd_mnt))
            except Exception as Ex:
                msg = f"\tshare path not created! Ex:{Ex}"
                if verbose: print(msg)
        else:
            msg = f'\tshare path exists already!'
            if verbose: print(msg)
    except Exception as Ex:
        msg = f'\tshare path not created ({pth_share})!\tEx:{Ex}'
        if verbose: print(msg)
    pth_prj = chk_mk_dir(dir=pth_share + '/' + nm_prj, verbose=verbose)
    pth_data = chk_mk_dir(dir=pth_prj + '/data', verbose=verbose)
    pth_log = chk_mk_dir(dir=pth_prj + '/log', verbose=verbose)
    pth_dct = dict(
        pth_prj=pth_prj,
        data_pth=pth_data + '/' + d['file_data'],
        log_pth=pth_log + '/' + d['file_log']
    )
    if verbose: bugprn(x='e')
    return pth_dct


def init_data_file(d, pth_dct, verbose=False):
    ''' initialize csvfiles for data in both local and shared directories
    '''
    msg = False
    if verbose: bugprn()
    pth_local = f'data/{d["file_data"]}'
    pth_share = pth_dct['data_pth']
    if not Path(pth_share).is_file():
        try:
            msg = f'pth_share: {pth_share}\n'
            if verbose: print(msg)
            with open(pth_share, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=d['col_nm'])
                writer.writeheader()
            msg = f'wrote to {pth_share}'
            if verbose: print(msg)
        except Exception as Ex:
            msg = f'Error initializing data file! Ex: {Ex}'
            if verbose: print(msg)
    if not Path(pth_local).is_file():
        try:
            msg = f'pth_local: {pth_local}\n'
            if verbose: print(msg)
            with open(pth_local, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=d['col_nm'])
                writer.writeheader()
            msg = f'wrote to {pth_local}'
            if verbose: print(msg)
        except Exception as Ex:
            msg = f'Error initializing local data file! Ex: {Ex}'
            print(msg)
    if verbose: bugprn(x='e')
    return msg


if __name__ == '__main__':
    if verbose: bugprn()
    msg = f'Press Ctrl-C to quit.'
    print(msg)
    # intialize data
    # Initial the dht device, with data pin connected to:
    # dictionary types are tuples and not lists....
    sensor_dict = dict(
        type='DHT11',
        instance=af.DHT11,
        data=('Humidity', 'Temperature'),
        pins=(4, 17, 23),
        id="DHT11_190",
        col_nm=())

    # determine the columns to output based on sensor and
    # number of data points and number of pins
    col_nm = ('Date',)
    if not isinstance(sensor_dict['pins'], tuple):
        sensor_dict['pins'] = (sensor_dict['pins'],)
    if not isinstance(sensor_dict["data"], tuple):
        sensor_dict["data"] = (sensor_dict["data"],)
    data_rng = range(0, len(sensor_dict["data"]), 1)
    for pin in sensor_dict['pins']:
        for i in data_rng:
            col_nm += (sensor_dict["data"][i] + '_' + str(pin),)
    col_nm += tuple(i + '_avg' for i in sensor_dict['data'])
    col_nm += (sensor_dict['type'],)

    # add calculated items to sensor dictionary
    sensor_dict['col_nm'] = col_nm
    sensor_dict['file_data'] = str(len(sensor_dict['pins'])) + \
        '_sensor_' + sensor_dict['type'] + '.csv'
    sensor_dict['file_log'] = sensor_dict['id'] + '.log'
    # add monitoring levels to sensor dictionary
    sensor_dict['threshold'] = .90
    sensor_dict['freq_min_reading'] = 20
    sensor_dict['freq_hr_writing'] = 1
    sensor_dict['freq_ctr'] = divmod(
        sensor_dict['freq_hr_writing'] * 60, sensor_dict['freq_min_reading'])[0]

    #####################################################################
    # Adjust settings to expedite testing results
    if verbose == True and testing == True:
        sensor_dict['freq_ctr'] = 2
        sensor_dict['freq_min_reading'] = 2
    ########################################################################

    # Google Docs spreadsheet info.
    oauth_g = 'gc.json'
    spreadsheet_g = 'ruminate_data'

    # initialize data as dictionary or tuple if possible
    data_dct = {}
    data_prev = ()
    for i in data_rng:
        data_prev += (None,)
    ctr = 0

    # Local vs shared drive
    # Attempt to mount shared drive
    pth_dct = mnt_share(d=sensor_dict, nm_prj='ruminate',
                        pth_share='/share/projects', verbose=verbose)
    msg = f'pth_dct: {pth_dct}'
    if verbose: print(msg)

    # check and create local data and log directory and (file headers)
    chk_mk_dir(dir='data', verbose=verbose)
    chk_mk_dir(dir='log', verbose=verbose)
    msg = init_data_file(d=sensor_dict, pth_dct=pth_dct, verbose=verbose)
    # Frist measurement is now
    measure_time = datetime.datetime.now().minute
    wait_time = datetime.datetime.now().minute + sensor_dict['freq_min_reading']

    # set up log handlers
    logger = logging.getLogger(sensor_dict['id'])
    logger.setLevel(logging.INFO)

    while True:
        time.sleep(1)

        # Attempt to mount shared drive
        pth_dct = mnt_share(d=sensor_dict, nm_prj='ruminate',
                            pth_share='/share/projects', verbose=verbose)
        # Attempt to take readings
        try:
            data = reading_sensors(d=sensor_dict, data_prev=data_prev,
                                   dir_dct=pth_dct, verbose=verbose)
            msg = f'data: {data}'
            if verbose: print(msg)
            # get average data
            data_avg = get_avg(d=sensor_dict, data=data, dir_dct=pth_dct, verbose=verbose)
            msg = f'\ndata_avg: {data_avg}\n'
            if verbose: print(msg)
        except Exception as Ex:
            msg = f'Trouble collecting and averaging data! Ex: {Ex}'
            print(msg)
            write_log(d=sensor_dict, dir_dct=pth_dct, logger=logger, txt=msg,
                      when='W6', interval=4, backupCount=5, verbose=verbose)
            time.sleep(5)
        # Create and add to data dictionary and data_tuple for saving to files
        # Google Sheets, local and shared data directories
        data_tuple = False
        ctr_d = 0
        while ctr_d < 5 and data_tuple == False:
            ctr_d += 1
            try:
                # record readings
                tm = datetime.datetime.now().isoformat()
                data_tuple = tuple(x for t in data.values() for x in t)
                data_tuple = (tm,) + data_tuple + data_avg + (sensor_dict['id'],)
                msg = f'data_tuple: {data_tuple}\nsensor_dict[col_nm]: {sensor_dict["col_nm"]}'
                if verbose: print(msg)
                data_dct[tm] = {n: d for n, d in zip(sensor_dict['col_nm'], data_tuple)}
                msg = f'\ndata_dct: \n{data_dct}\n'
                if verbose: print(msg)
                data_prev = data_avg
                msg = f'data_prev {data_prev}'
                if verbose: print(msg)
                if verbose:
                    time.sleep(2)
                else:
                    time.sleep(33)
                # Add to counter when record is valid
                ctr += 1
            except Exception as Ex:
                msg = f'Error recording data to dictionary! Ex:{Ex}'

        if ctr_d > 4:
            print(msg)
            write_log(d=sensor_dict, dir_dct=pth_dct, logger=logger, txt=msg,
                      when='W6', interval=4, backupCount=5, verbose=verbose)

        msg = f'\n\tdata_tuple: {data_tuple}\n'
        if verbose: print(msg)

        # Connect and append to google sheet
        worksheet = False
        ctr_w = 0
        msg = False
        while ctr_w < 5 and worksheet == False:
            ctr_w += 1
            try:
                time.sleep(1)
                worksheet = login_open_sheet(d=pth_dct, oauth_file=oauth_g,
                                             spreadsheet=spreadsheet_g, verbose=verbose)
                msg = f'login_open_sheet to worksheet on {ctr_w} try'
                if verbose: print(msg)
                worksheet.append_row(data_tuple)
                time.sleep(1)
            except Exception as Ex:
                msg = f'Error opening spreadsheet or appending! ctr_w:{ctr_w} Ex: {Ex}'
                time.sleep(1)
        if ctr_w > 4 or worksheet == False:
            if msg == False:
                msg = f'Error accessing spreadsheet! ctr_w:{ctr_w}'
            print(msg)
            write_log(d=sensor_dict, dir_dct=pth_dct, logger=logger, txt=msg, when='W6', interval=4, backupCount=5, verbose=verbose)
        msg = f'worksheet\n{type(worksheet)}'
        if verbose: print(msg)

        # Attempt to mount shared drive
        pth_dct = mnt_share(d=sensor_dict, nm_prj='ruminate',
                            pth_share='/share/projects', verbose=verbose)
        msg = f'ctr mod freq_ctr ... {wait_time} ... {ctr % sensor_dict["freq_ctr"]}'
        if verbose: print(msg)

        # Save data dictionary to data files in shared and local data folders
        if ctr % sensor_dict['freq_ctr'] == 0:
            filesave = False
            ctr_s = 0
            while ctr_s < 5 and filesave == False:
                ctr_s += 1
                # Attempt to mount shared drive
                pth_dct = mnt_share(d=sensor_dict, nm_prj='ruminate',
                                    pth_share='/share/projects', verbose=verbose)
                init_data_file(d=sensor_dict, pth_dct=pth_dct, verbose=verbose)

                try:
                    key_first = list(data_dct.keys())[0]
                    fieldnames = data_dct[key_first].keys()
                    msg = f'fieldnames: {fieldnames}'
                    if verbose: print(msg)
                    msg = f'col_nm: {sensor_dict["col_nm"]}'
                    if verbose: print(msg)
                    time.sleep(1)
                    with open(pth_dct['data_pth'], 'a', newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        for k in data_dct:
                            msg = f'k: {k}\ndata_dctk:\n{data_dct[k]}'
                            if verbose: print(msg)
                            writer.writerow(data_dct[k])
                        time.sleep(1)
                    data_dct = {}
                    filesave = True
                except Exception as Ex:
                    msg = f'Error writing data to file! Ex: {Ex}'
            if ctr_s > 4:
                print(msg)
                write_log(d=sensor_dict, dir_dct=pth_dct, logger=logger, txt=msg,
                          when='W6', interval=4, backupCount=5, verbose=verbose)

        # Next measurement time
        try:
            measure_time = datetime.datetime.now().minute
            wait_time = next_measurement(time_init=measure_time,
                                         freq_min=sensor_dict['freq_min_reading'])
        except Exception as Ex:
            msg = f'Error creating new measurement time! Ex: {Ex}'
            print(msg)
            write_log(d=sensor_dict, dir_dct=pth_dct, logger=logger, txt=msg,
                      when='W6', interval=4, backupCount=5, verbose=verbose)
            raise
    os.system('sudo reboot')
else:
    print('Reboot System')
