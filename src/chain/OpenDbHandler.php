<?php


namespace dsj\sync\console\chain;

use dsj\sync\web\models\TSyncDb;
use dsj\sync\web\models\TSyncTask;
use yii\db\Connection;

class OpenDbHandler extends AbsHandler
{

    /**
     * @param TSyncTask $data_obj
     * @param $source_db Connection
     * @param $aid_db Connection
     * @return mixed|void
     * @throws \yii\db\Exception
     */
    public function handlerSync($data_obj, $source_db, $aid_db)
    {
        $sourceDbModel = TSyncDb::findOne(['id' => $data_obj->source_db_id]);

        $sourceDb = TSyncDb::getActiveDb($sourceDbModel);

        $aidDbModel = TSyncDb::findOne(['id' => $data_obj->aid_db_id]);

        $aidDb = TSyncDb::getActiveDb($aidDbModel);

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj, $sourceDb, $aidDb);
        }
    }
}