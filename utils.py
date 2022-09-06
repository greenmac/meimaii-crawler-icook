from datetime import datetime
import time


def timer(func):
    def wrap(time_sleep):
        start_time_dt =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
        print('='*60)

        func(time_sleep)

        end_time = time.time()
        end_time_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cost_time = end_time-start_time
        m, s = divmod(cost_time, 60)
        h, m = divmod(m, 60)
        cost_time_dt = f'{int(h)}h:{int(m)}m:{round(s, 2)}s'
        
        print('='*60)
        print('start_time:', start_time_dt)
        print('end_time  :', end_time_dt)
        print('cost_time :', cost_time_dt)
        return func(time_sleep)
    return wrap