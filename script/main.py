import datetime
import boto3
import os
from dateutil.relativedelta import relativedelta
import logging

from script.settings import Settings


class S3Control:
    def __new__(cls):
        cls.s3 = boto3.client(
            service_name=Settings.S3_CONTROL_SETTINGS["service_name"],
            endpoint_url=Settings.S3_CONTROL_SETTINGS["endpoint_url"],
            region_name=Settings.S3_CONTROL_SETTINGS["region_name"],
            aws_access_key_id=Settings.S3_CONTROL_SETTINGS["aws_access_key_id"],
            aws_secret_access_key=Settings.S3_CONTROL_SETTINGS["aws_secret_access_key"],
        )
        cls.bucket_name = Settings.S3_CONTROL_SETTINGS["bucket_name"]

        logging.info(
            "Подключение к {} Bucket".format(
                Settings.S3_CONTROL_SETTINGS["bucket_name"]
            )
        )

        return super(S3Control, cls).__new__(cls)

    def _pars_file(self, list_data: list) -> list:
        """Основной алгоритм для вычисления ненужных файлов. оставляет только:
        все файлы этого и прошлого месяца + по одному самому раннему файлу более поздних месяцев кажждой бд.
        Возвращает список файлов на удаление.
        Обязательные для передачи аргументы:
        1) list_data - Список файлов на обработку"""
        pre_del_files = []
        del_files = []
        save_month = []
        safety_bag = (
            datetime.datetime.now().date() - relativedelta(months=1)
        ).strftime("%Y-%m")
        for file in list_data:
            if file[:7:] <= safety_bag:
                pre_del_files.append(file)
        pre_del_files = sorted(pre_del_files)
        for file_on_del in pre_del_files:
            db_name = ""
            if len(file_on_del.split("-")) == 4:
                db_name = file_on_del.split("-")[3][:-7:]
            if file_on_del[:7:] + db_name not in save_month:
                save_month.append(file_on_del[:7:] + db_name)
            else:
                del_files.append(file_on_del)
        return del_files

    def get_all_files_in_bucket(self) -> list:
        """Возвращает все файлы из bucket"""
        result_list = []
        for key in self.s3.list_objects(Bucket=self.bucket_name)["Contents"]:
            result_list.append(key["Key"])
        return result_list

    def get_files_from_bucket_dirs(self, dir_name: str) -> list:
        """Возвращает из Bucket файлы по заданному имени директории.
        Обязательные для передачи аргументы:
        1) dir_name - Имя директории Bucket"""
        result_list = []
        len_dir_name = len(dir_name)
        for key in self.s3.list_objects(Bucket=self.bucket_name)["Contents"]:
            if (
                key["Key"][len_dir_name::] != ""
                and key["Key"][: len_dir_name + 1 :] == dir_name + "/"
                and len(key["Key"]) != len_dir_name + 1
            ):
                result_list.append(key["Key"][len_dir_name + 1 : :])
        return result_list

    def get_files_from_bucket_dir(self, dir_name: str) -> list:
        """Возвращает из Bucket файлы по заданному имени директории,не затрагивая файлы
        внутрилежащих директорий.
        Обязательные для передачи аргументы:
        1) dir_name - Имя директории Bucket"""
        result_list = []
        len_dir_name = len(dir_name)
        num_file_in_name = len(dir_name.split("/")) + 1
        for key in self.s3.list_objects(Bucket=self.bucket_name)["Contents"]:
            if (
                key["Key"][len_dir_name::] != ""
                and key["Key"][: len_dir_name + 1 :] == dir_name + "/"
                and len(key["Key"]) != len_dir_name + 1
            ):
                file = key["Key"].split("/")
                if len(file) < num_file_in_name + 1:
                    result_list.append(file[num_file_in_name - 1])
        return result_list

    def delete_all_files_in_dir(self, dir_name: str) -> None:
        """Удаляет файлы строго по заданному имени директории.
        Обязательные для передачи аргументы:
        1) dir_name - Имя директории Bucket"""
        files_on_del = self.get_files_from_bucket_dirs(dir_name)
        for file_del in files_on_del:
            self.s3.delete_object(
                Bucket=self.bucket_name, Key=dir_name + "/" + file_del
            )
            logging.info("Файл {} был удален".format(dir_name + "/" + file_del))

    def delete_files_without_dirs(self, dir_name: str) -> None:
        """Удаляет файлы строго по заданному имени директории, не затрагивая файлы
        файлы внутрилежащих директорий.
        Обязательные для передачи аргументы:
        1) dir_name - Имя директории Bucket"""
        files_on_del = self.get_files_from_bucket_dir(dir_name)
        for file_del in files_on_del:
            self.s3.delete_object(
                Bucket=self.bucket_name, Key=dir_name + "/" + file_del
            )
            logging.info("Файл {} был удален".format(dir_name + "/" + file_del))

    def del_files_from_bucket_with_algorithm(self, dir_name: str) -> None:
        """Удаляет файлы строго по заданному имени директории с использованием алгоритма.
        Обязательные для передачи аргументы:
        1) dir_name - Имя директории Bucket"""
        result_list = self.get_files_from_bucket_dirs(dir_name)
        files_on_delete = self._pars_file(result_list)
        for file_del in files_on_delete:
            self.s3.delete_object(
                Bucket=self.bucket_name, Key=dir_name + "/" + file_del
            )
            logging.info("Файл {} был удален".format(dir_name + "/" + file_del))

    def recursion_put_data_in_bucket(
        self, bucket_dir_name: str, path_to_dir: str
    ) -> None:
        """Рекурсивно добавляет файлы в bucket.
        Обязательные для передачи аргуметы:
        1) bucket_dir_name - имя начальной дириктории загрузки
        2) path_to_dir - путь к директории
        """
        my_bucket_dir_name = bucket_dir_name
        for file_name in os.listdir(path_to_dir):
            path_to_dir_file = path_to_dir + "/" + file_name
            if os.path.isdir(path_to_dir_file):
                new_bucket_dir_name = my_bucket_dir_name + "/" + file_name
                self.recursion_put_data_in_bucket(
                    bucket_dir_name=new_bucket_dir_name, path_to_dir=path_to_dir_file
                )
                continue
            with open(path_to_dir_file, "rb") as data:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=my_bucket_dir_name + "/" + file_name,
                    Body=data,
                )
            logging.info(
                "Рекурсивно загружен файл {} ".format(
                    my_bucket_dir_name + "/" + file_name
                )
            )

    def put_data_in_bucket(self, bucket_dir_name: str, path_to_dir: str) -> None:
        """Загружает в бакет все файлы,за исключением внутрилежащих директорий,
        внутри указанной директории на локальном хранилище(пк)
        и сохраняет в указанную директорию Bucket.
        Обязательные для передачи аргументы:
        1) bucket_dir_name - имя директории куда сохраняются файлы
        2) path_to_dir - путь к директории на локальном хранилище"""
        for file_name in os.listdir(path_to_dir):
            file = path_to_dir + "/" + file_name
            if not os.path.isdir(file):
                with open(file, "rb") as data:
                    self.s3.put_object(
                        Bucket=self.bucket_name,
                        Key=bucket_dir_name + "/" + file_name,
                        Body=data,
                    )
                logging.info(
                    "Загружен файл {}".format(bucket_dir_name + "/" + file_name)
                )

    def upload_data_in_bucket(self, bucket_dir_name, path_to_dir):
        for file_name in os.listdir(path_to_dir):
            file = path_to_dir + "/" + file_name
            if not os.path.isdir(file):
                self.s3.upload_file(
                    file,
                    self.bucket_name,
                    bucket_dir_name + "/" + file_name,
                    Config=Settings.config,
                )
                logging.info(
                    "U: Загружен файл {}".format(bucket_dir_name + "/" + file_name)
                )

    def recursion_upload_data_in_bucket(
        self, bucket_dir_name: str, path_to_dir: str
    ) -> None:
        """Рекурсивно добавляет файлы в bucket.
        Обязательные для передачи аргуметы:
        1) bucket_dir_name - имя начальной дириктории загрузки
        2) path_to_dir - путь к директории
        """
        my_bucket_dir_name = bucket_dir_name
        for file_name in os.listdir(path_to_dir):
            path_to_dir_file = path_to_dir + "/" + file_name
            if os.path.isdir(path_to_dir_file):
                new_bucket_dir_name = my_bucket_dir_name + "/" + file_name
                self.recursion_upload_data_in_bucket(
                    bucket_dir_name=new_bucket_dir_name, path_to_dir=path_to_dir_file
                )
                continue
            self.s3.upload_file(
                path_to_dir_file,
                self.bucket_name,
                bucket_dir_name + "/" + file_name,
                Config=Settings.config,
            )
            logging.info(
                "U: Рекурсивно загружен файл {} ".format(
                    my_bucket_dir_name + "/" + file_name
                )
            )


if __name__ == "__main__":
    s3 = S3Control()
    # s3.get_files_from_bucket_dirs('test')
    # s3.get_files_from_bucket_dir('test')
    # s3.get_all_files_in_bucket()
    # s3.delete_all_files_in_dir('test')
    # s3.delete_files_without_dirs('test/beta/gamma')
    # s3.del_files_from_bucket_with_algorithm('test/alpha')
    # s3.upload_data_in_bucket('files', '../files')
    # s3.recursion_upload_data_in_bucket('test', '../files')
    # s3.upload_data_in_bucket('test', '../files')
    # s3.recursion_upload_data_in_bucket('test', '../files')
