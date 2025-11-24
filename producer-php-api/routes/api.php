<?php

use App\Http\Controllers\Api\OrderController;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;


Route::get('/healthcheck', function () {
    return response()->json([
        'status' => 'ok',
    ]);
});

Route::post('/v1/orders', [OrderController::class, 'store']);