<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;


Route::get('/healthcheck', function () {
    return response()->json([
        'status' => 'ok',
    ]);
});
