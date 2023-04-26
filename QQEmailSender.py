import smtplib
from email.mime.text import MIMEText
from email.header import Header


class Mail:
    def __init__(self):
        # 第三方 SMTP 服务
        self.mail_host = "smtp.qq.com"  # 设置服务器:这个是qq邮箱服务器，直接复制就可以
        self.mail_pass = "npnmclyyssgtdaac"  # 刚才我们获取的授权码
        self.sender = '1905561110@qq.com'  # 你的邮箱地址
        self.receivers = [self.sender]  # 收件人的邮箱地址，可设置为你的QQ邮箱或者其他邮箱

    def send(self, content='https://github.com/zuoxiaolei/eft_trade'):
        # 随机获取四位数的验证码
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = Header("量化交易策略提醒", 'utf-8')
        message['To'] = Header("1905561110@qq.com", 'utf-8')

        subject = '量化交易策略提醒'  # 发送的主题，可自由填写
        message['Subject'] = Header(subject, 'utf-8')
        try:
            smtpObj = smtplib.SMTP_SSL(self.mail_host, 465)
            smtpObj.login(self.sender, self.mail_pass)
            smtpObj.sendmail(self.sender, self.receivers, message.as_string())
            smtpObj.quit()
            print('邮件发送成功')
        except smtplib.SMTPException as e:
            import traceback
            traceback.print_exc()
            print('邮件发送失败')


if __name__ == '__main__':
    mail = Mail()
    mail.send()
