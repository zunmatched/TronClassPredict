# TronClassPredict

從本校的線上課程平台下載學生的學習足跡資料，透過深度學習演算法(使用PyTorch)預測學生在該課程的最終成績(成績預測模型)以及建議下周學習進度(下周進度模型)。

將預測成績以linebot推撥給各學生，並讓學生自己選擇想要進步的幅度(提升預測分數10分或者以80分為目標)，並追蹤學生在linebot上的點擊紀錄以及TronClass的下周學習進度，判斷學生對於推送內容的反應程度。

訓練過程：使用所有數據訓練出標準模型，再根據各課程對標準模型做優化。這樣可以讓初次使用TronClass的課程也能使用該功能，不需提供訓練資料。

(由於保密協議及系統設定，此處僅提供訓練模型用數據，並打碼處理程式碼中的帳號密碼及部分網址)
