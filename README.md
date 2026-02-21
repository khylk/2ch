## Установка

```bash
git clone https://github.com/khylk/2ch.git && cd 2ch
python -m venv venv && . ./venv/bin/activate
pip install -U pip && pip install -r requirements.txt
```

## Использование

```bash
python main.py <папка> <тип> <ссылки...>
```

| Аргумент | Значения | Описание |
|----------|----------|----------|
| `папка`  | любой путь | куда сохранять файлы |
| `тип`    | `img` · `vid` · `both` | что скачивать |
| `ссылки` | одна или несколько | треды 2ch / arhivach |

```bash
python main.py ~/Downloads/threads both \
  https://2ch.su/b/res/1.html \
  https://arhivach.vc/thread/2/
```

Файлы каждого треда сохраняются в отдельную подпапку: `2ch_1/`, `arhivach_2/` и т.д.
