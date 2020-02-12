<?php


namespace dsj\sync\console\chain;

use dsj\sync\web\models\TSyncTask;
use Yii;
use yii\db\Connection;

class CloseDbHandler extends AbsHandler
{

    /**
     * @param TSyncTask $data_obj
     * @param $source_db Connection
     * @param $aid_db Connection
     * @return mixed|void
     */
    public function handlerSync($data_obj, $source_db, $aid_db)
    {
        $config = \Yii::$app->getComponents()['db'];
        Yii::$app->set('db', $config);

        $data_obj->end_timestamp = time();
        $data_obj->execute_time = $data_obj->end_timestamp - $data_obj->start_timestamp;
        $data_obj->status = 3;
        $data_obj->save();

        $source_db->close();

        $aid_db->close();

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj, $source_db, $aid_db);
        }
    }
}