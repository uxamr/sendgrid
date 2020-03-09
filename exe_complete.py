from sendgrid import sendgrid

def __main__() :
    sendgrid().messages()
    sendgrid().stats()
    sendgrid().geo_stats()

if __name__ == "__main__" :
    __main__()