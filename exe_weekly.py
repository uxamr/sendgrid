from sendgrid import sendgrid
from utils import Semana

def __main__() :

    fecha = Semana().fechas
    sendgrid().messages(start_date = fecha[0], end_date = fecha[1])
    sendgrid().stats(start_date = fecha[0], end_date = fecha[1])
    sendgrid().geo_stats(start_date = fecha[0], end_date = fecha[1])

if __name__ == "__main__" :
    __main__()