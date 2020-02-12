<?php


namespace dsj\sync\console\chain;


use dsj\sync\web\models\TSyncTask;

abstract class AbsHandler
{
    protected $nextHandler = null;

    public function setNextHandler($nextHandler){
        $this->nextHandler = $nextHandler;
    }

    /**
     * @param $data_obj TSyncTask
     * @return mixed
     */
    public abstract function handlerSync($data_obj,$source_db,$aid_db);
}