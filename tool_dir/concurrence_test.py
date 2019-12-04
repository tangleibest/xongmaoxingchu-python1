# from locust import HttpLocust,TaskSet,task
#
# """
# 创建后台管理站点压测类，需要继承TaskSet
# 可以添加多个测试任务
# """
# class AdminLoadTest(TaskSet):
#
#     # 用户执行task前调用
#     def on_start(self):
#         pass
#
#     # 用户执行task后调用
#     def on_stop(self):
#         pass
#
#     @task
#     def download(self):
#         # 头部
#         header = {"key":"value"}
#         # 参数
#         data = {"key":"value"}
#         self.client.get('http://140.143.28.107:5001/api/getCate?coordinate=116.494325,39.976051&distance=1.5&city_id=1&platform=elm')
#
# class RunLoadTests(HttpLocust):
#     """
#     创建运行压测类
#     """
#     task_set = AdminLoadTest
#     min_wait = 1000
#     max_wait = 50000
#
#
# if __name__ == "__main__":
#     import os
#     os.system("locust -f F:\\python\\xongmaoxingchu-python\\tool_dir\\concurrence_test.py --host=http://140.143.28.107:5001")