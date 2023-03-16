Задания:

Сначала определим год, за какой будем смотреть данные.

Сделать это можно так:
в питоне выполнить 1994 + hash(f‘{login}') % 23,  где {login} - ваш логин (или же папка с дагами)

Дальше нужно составить DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:

1. Какая игра была самой продаваемой в этом году во всем мире?
2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
Перечислить все, если их несколько
4. У какого издателя самые высокие средние продажи в Японии?
Перечислить все, если их несколько
5. Сколько игр продались лучше в Европе, чем в Японии?
Оформлять DAG можно как угодно, важно чтобы финальный таск писал в лог ответ на каждый вопрос. Ожидается, что в DAG будет 7 тасков. По одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы. 
