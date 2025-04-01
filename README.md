# Анализ показателей сервиса доставки еды

## Cтек использованных технологий 
<img src="https://github.com/devicons/devicon/blob/master/icons/postgresql/postgresql-original.svg" height="40"/><img src="https://336118.selcdn.ru/Gutsy-Culebra/products/Redash-Logo.png" height="40"/>

## Основные цели 
  В сервис по покупке продуктов питания была добавлена функция оформления доставки заказа на дом через приложение либо веб-версию. Для привлечения клиентов параллельно было запущено 2 рекламные компании. Позже, в сентябре, был произведен ряд изменений, которые были связаны с расширением площадей и изменением мотивации сотрудников.
  
  Необходимо было расчетать основные метрики по данным работы сервиса и их последующая визуализация.
#### Этапы работы:
1 Расчет и визуализация метрик оценки аудитории сервиса (Кол-во новых пользователей / курьеров, кол-во активных пользователей / курьеров, кол-во заказов и др.).<br>
2.1 Расчет и визуализация экономических метрик сервиса (Выручка, валовая прибыль и др.).<br>
2.2 Расчет и визуализация маркетинговые метрик сервиса для проведенных рекламных компаний (CAC, ROI, Retention и др.).<br>

## Полученные результаты 

#### Метрик оценки аудитории:
5 и 6 сентября наблюдалось существенное снижение метрик, связаных с количеством платящих клиентов. На остальном рассматриваемом промежутке времени показатели более стабильны либо имеют положительную динамику.    
Лучшие показатели по заказам наблюдаются в вечернее время после 17. Время ожидания доставки соответствует целевым показателям, но нагрузка на курьеров очень мала. Необходимо принимать меры для увеличения количества оформляемых заказов либо регулировать количество курьеров, работающих в системе.

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/fsmhbQZle6FJcfnE8HbhLC0rSu8cGjclmHdjpTV9?org_slug=default)

#### Экономические метрики:
Из товаров самую большую долю в выручке занимает свинина и мясо в целом.
5 и 6 сентября наблюдалось снижение выручки, что объясняется снижением количества платящих клиентов. Начиная с 31 Августа ежедневная валовая прибыль сервиса стала положительной, а суммарная валовая прибыль превысила нулевую отметку 6 сентября, и сервис впервые «вышел в плюс» по этому показателю.
Оптимизация стоимости сборки заказа в сентябре позволила увидеть в этом месяце положительную валовую прибыль.

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/YWpHWm47j9vGZwqK4Cnznih0t6mLW5II3M9DDHdq?org_slug=default)

#### Маркетинговые метрики:

Рекламная компания № 1 более эффективна. Доход от заказов покупателей, пришедших после проведения рекламной кампании № 1, превысил расходы на их привлечение на 5й день, а у кампании № 2 этот этот показатель не превысил расходы на привлечение клиентов вовсе за наблюдаемый период. Также Кампании № 2 имеет более низкий Retention по сравнению с кампанией № 1 и в сравнении с аналогичным показателем по всем пользователям.

[ВИЗУАЛИЗАЦИЯ](https://redash.public.karpov.courses/public/dashboards/P4EvKqOhIqJ67Alvxndt58RyQZy4Sp5f1G43LLTX?org_slug=default)
