<?php


namespace dsj\sync\console\chain;

use dsj\components\helpers\LogHelper;
use dsj\sync\web\models\TSyncTask;
use yii\db\Connection;
use yii\helpers\Json;

/**
 * Class CheckAidTablesHandler
 * @package console\modules\sync\chain
 * 检查目标数据库表是否存在，不存在则自动创建
 */
class CheckAidTablesHandler extends AbsHandler
{

    /**
     * @param TSyncTask $data_obj
     * @param $source_db Connection
     * @param $aid_db Connection
     * @return mixed|void
     */
    public function handlerSync($data_obj,$source_db,$aid_db)
    {
        $syncTables = Json::decode($data_obj->sync_tables,true);

        foreach ($syncTables as $item){
            try{
                $aid_db->createCommand("select * from {$item}")->queryOne();
            }catch (\Exception $e){
                (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                    ->setData("目标数据库中<{$item}>表不存在，将自动创建")
                    ->write();
                $extra = Json::decode($data_obj->extra);
                $extra[] = "create table {$item}";
                $data_obj->extra = Json::encode($extra);
                $data_obj->end_timestamp = time();
                $data_obj->execute_time = $data_obj->end_timestamp - $data_obj->start_timestamp;
                $data_obj->status = 2;
                $data_obj->save();

                $createTableSql = $source_db->createCommand("show create table {$item}")->queryOne()['Create Table'];
                $aid_db->createCommand($createTableSql)->execute();
            }
        }

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj,$source_db,$aid_db);
        }
    }
}