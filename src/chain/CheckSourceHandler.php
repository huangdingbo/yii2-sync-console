<?php


namespace dsj\sync\console\chain;

use dsj\components\helpers\LogHelper;
use dsj\sync\web\models\TSyncDb;
use yii\helpers\Json;

class CheckSourceHandler extends AbsHandler
{
    public function handlerSync($data_obj,$source_db,$aid_db)
    {
        try{
            $sourceDbModel = TSyncDb::findOne(['id' => $data_obj->source_db_id]);

            TSyncDb::getActiveDb($sourceDbModel);

            (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                ->setData("源数据库<" . $sourceDbModel->name . ">连接成功")
                ->write();
        }catch (\Exception $e){
            (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                ->setData("<" . $sourceDbModel->name . ">源数据库连接错误,请检查。error:"  . $e->getMessage())
                ->write();
            $extra = Json::decode($data_obj->extra);
            $extra[] = 'source_db connect error';
            $data_obj->extra = Json::encode($extra);
            $data_obj->status = 2;
            $data_obj->end_timestamp = time();
            $data_obj->save();
            $this->nextHandler = null;
        }

        try{
            $aidDbModel = TSyncDb::findOne(['id' => $data_obj->aid_db_id]);

            TSyncDb::getActiveDb($aidDbModel);

            (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                ->setData("目标数据库<" . $aidDbModel->name . ">连接成功")
                ->write();
        }catch (\Exception $e){
            (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                ->setData("<" . $aidDbModel->name . ">目标数据库连接错误,请检查。error:"  . $e->getMessage())
                ->write();
            $extra = Json::decode($data_obj->extra);
            $extra[] = 'aid_db connect error';
            $data_obj->extra = Json::encode($extra);
            $data_obj->status = 2;
            $data_obj->end_timestamp = time();
            $data_obj->save();
            $this->nextHandler = null;
        }

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj,$source_db,$aid_db);
        }
    }
}