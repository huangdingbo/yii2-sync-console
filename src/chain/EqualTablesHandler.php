<?php


namespace dsj\sync\console\chain;

use dsj\components\helpers\LogHelper;
use dsj\sync\web\models\TSyncTask;
use yii\db\Connection;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

/**
 * Class EqualTables
 * @package console\modules\sync\chain
 * 检查源数据库和目标数据库数据表是否相同
 */
class EqualTablesHandler extends AbsHandler
{

    /**
     * @param TSyncTask $data_obj
     * @param $source_db Connection
     * @param $aid_db Connection
     * @return mixed|void
     */
    public function handlerSync($data_obj,$source_db,$aid_db)
    {
        $tables = Json::decode($data_obj->sync_tables,true);

        foreach ($tables as $item){
            $sourceTable = $source_db->createCommand("desc {$item}")->queryAll();
            $aidTable = $aid_db->createCommand("desc {$item}")->queryAll();
            $diffArr = $this->array_diff_2d($aidTable,$sourceTable);
            $extra = Json::decode($data_obj->extra);
            if ($diffArr){
                foreach ($diffArr as $value){
                    foreach ($value as $key => $val){
                        $extra[] = "aid_db field {$key} difference";
                        (new LogHelper())->setRoute(__CLASS__)->setFileName('sync.txt')
                            ->setData("目标数据库id:<{$data_obj->aid_db_id}>中的数据表<{$key}>与源数据库存在差异，请检查")
                            ->write();
                    }
                }
                $data_obj->extra = Json::encode($extra);
                $data_obj->end_timestamp = time();
                $data_obj->execute_time = $data_obj->end_timestamp - $data_obj->start_timestamp;
                $data_obj->status = 2;
                $data_obj->save();
                $this->nextHandler = null;
            }
        }

        if ($this->nextHandler != null){
            $this->nextHandler->handlerSync($data_obj,$source_db,$aid_db);
        }
    }

    /**
     * @param $arr1
     * @param $arr2
     * @return array
     * 得到的是arr1 对应 arr2 的差异,返回的是arr1的值
     */
    private function array_diff_2d($arr1,$arr2){
        $arr1Map = ArrayHelper::index($arr1,'Field');
        $arr2Map = ArrayHelper::index($arr2,'Field');

        $diffArr = [];

        foreach ($arr1Map as $key => $item){
            $diff = array_diff($item,$arr2Map[$key]);
            if ($diff){
                $diffArr[] = $diff;
            }
        }

        return $diffArr;
    }
}