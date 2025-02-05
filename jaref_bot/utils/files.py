def load_tokens_from_file(file_name):
    """
    Считывает список токенов из текстового файла.

    :param file_name: Имя файла с токенами.
    :return: Список токенов.
    """
    try:
        with open(file_name, "r", encoding="utf-8") as file:
            tokens = file.read().splitlines()
        return tokens
    except FileNotFoundError:
        print(f"Файл {file_name} не найден.")
        return []
