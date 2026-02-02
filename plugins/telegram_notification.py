from airflow.providers.telegram.operators.telegram import TelegramOperator

class TelegramNotification:
    @staticmethod
    def send_message_error(context):
        state = context.get('ti').state
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        logical_date = context.get("logical_date")

        message = f"{state} 😱\nDAG: {dag_id} \nTask: {task_id} \nExecution date: {logical_date}"

        send_message = TelegramOperator(
            task_id='send_message_telegram',
            telegram_conn_id='telegram_id',
            chat_id='-1003370928329',
            text=message
        )
        return send_message.execute(context=context)