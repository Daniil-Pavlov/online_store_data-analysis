# Анализ показателей интернет магазина

## Cтек использованных технологий 
<img src="https://github.com/devicons/devicon/blob/master/icons/postgresql/postgresql-original.svg" height="40"/><img src="https://336118.selcdn.ru/Gutsy-Culebra/products/Redash-Logo.png" height="40"/>

## Основные цели 
  
Целью работы было расчет основных метрик по данным работы интернет магазина у которого имеется услуга доставки и их последующая визуализация.  

## Полученные результаты 

1.1 <br>
Количество пользователей растет быстрее чем количество курьеров на рассматриваемом промежутке времени.<br>
В данных присутствуют дни, когда показатели сильно выбивались из общей динамики. В частности существенное уменьшение числа новых пользователей 6 Сентября.<br>
Показатели числа новых курьеров более стабильный по сравнению с показателем числа новых пользователей.1.1 <br>
1.2 <br>  
Темпы прироста общего числа пользователей и курьеров стабильно снижаются на рассматриваемом промежутке времени.<br>
На рассматриваемом промежутке времени зачастую темп прироста числа новых пользователей заметно опережал темп прироста числа новых курьеров (Кроме 30, 31 Августа и 4, 6, 7 Сентября).
Глядя на графики с относительными показателями можно сказать, что показатель числа новых пользователей более стабилен, чем показатель числа новых курьеров.1.1 <br>
1.3 <br>
Вместе с общим числом пользователей и курьеров растёт число платящих пользователей и активных курьеров.<br>
Доля активных курьеров стабильна, а доля платящих пользователей имеет отрицательную динамику.1.1 <br>
1.4 <br>
В среднем доля пользователей с несколькими заказами держится в районе 28%.<br>
Минимальное значение наблюдается 24.08.22 (7%), что закономерно так как это первый день работы магазина.1.1 <br>
1.5 <br>
Абсолютных показатели имеют положительную динамику. С ростом количества всех заказов растут показатели числа первых заказов и числа заказов новых пользователей.<br>
Относительные показатели имеют отрицательную динамику, что вполне закономерно и они будут снижаться в дальнейшем.1.1 <br>
1.6 <br>
Динамика показатели числа платящих пользователей на одного активного курьера и числа заказов на одного активного курьера совпадают, что закономерно так как они взаимосвязаны.<br>
Нагрузка у курьеров нашего сервиса недостаточна. Сервису нестоит продолжать увеличивать количество курьеров.1.1 <br>
1.7 <br>
Время ожидания доставки заказа в сервисе в районе 20 мин. Большинство курьеров придерживаются этого целевого показателя.1.1 <br>
1.8 <br>
Пиковое значения числа оформляемых заказов наблюдается в 19 чесов, а минимальное в 4 часа.<br>
Прослеживается взаимосвязь между количеством оформляемых заказов и долей отменённых заказов. При пиковых значениях количества заказов наблюдается снижение cancel rate.1.1 <br>

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/fsmhbQZle6FJcfnE8HbhLC0rSu8cGjclmHdjpTV9?org_slug=default)

2.1.1<br>
5 и 6 сентября наблюдалось снижение выручки. В эти же дни наблюдалось значительное снижение количества новых клиентов. Так как большая часть наших клиентов делают долько 1 заказ этот фактор значительно отразился на выручке.<br>
2.1.2<br>
Метрика AOV более стабильна и не имеет сильных отклоненний на большей части рассматриваемого периода в отличие от ARPU и ARPAU.<br>
ARPU и ARPAU имеют несколько периодов снижения, которые совпадают с динамикой числа платящих пользователей. <br>
Доля платящих пользователей значительно не изменяется. <br>
2.1.3<br>
Running ARPU и Running ARPPU имеют положительную динамику и ростут на всем рассматриваемом периоде. Метрика Running AOV стабильна и колеблется в районе 383.<br>
Со временем растёт число заказов на одного пользователя.<br>
2.1.4<br>
Метрики ARPU и ARPPU принимали наибольшие значения в субботу, что согласуется со стандартным поведением пользователей сервиса доставки еды.<br>
Средний чек существенно не изменяется в зависимости от дня, а рост ARPU и ARPPU  обусловлен увеличением количества клиентов.<br>
2.1.5<br>
Доля выручки с заказов новых пользователей в общей выручке снижается, что закономерно так как ростет общее количество клиентов и старые клиенты также активны.<br>
2.1.6<br>
Свинина является товаром с наибольшей долей в выручке и в целом мясо как группа товаров является самой популярной.<br>
2.1.7<br>
Начиная с 31 Августа ежедневная валовая прибыль сервиса стала положительной.<br>
6 Сентября суммарная валовая прибыль превысила нулевую отметку и сервис впервые «вышел в плюс» по этому показателю.<br>
Оптимизация стоимости сборки заказа в сентябре позволила увидеть в этом месяце положительную валовую прибыль<br>

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/YWpHWm47j9vGZwqK4Cnznih0t6mLW5II3M9DDHdq?org_slug=default)

2.2.1<br>
У какой рекламной кампании затраты на привлечение одного покупателя оказались ниже?<br>
2.2.2<br>
Какой вывод об эффективности рекламных кампаний можно сделать? В какой канал привлечения имеет смысл вкладывать больше бюджета?<br>
2.2.3<br>
Что можно сказать о среднем чеке пользователей в двух группах? Можно ли сказать, что в одной из групп он значительно выше, чем во второй?<br>
2.2.4<br>
Нет вопроса<br>
2.2.5<br>
Так почему же две рекламные кампании отличаются по значению метрики ROI? Какой теперь вывод можно сделать?
2.2.6<br>
Какой вывод можно сделать на основе построенных графиков?<br>

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/YWpHWm47j9vGZwqK4Cnznih0t6mLW5II3M9DDHdq?org_slug=default)
